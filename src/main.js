const {generateUid} = require('./uid');
const moment = require('moment');
const XLSX = require('xlsx');
const rq = require('request-promise');
const _ = require('lodash');
const fs = require("fs");
const URL = require('url').URL;
const isReachable = require('is-reachable');
const winston = require('./winston');
const scheduler = require('node-schedule');

const mapping = require('./mapping.json');
mapping.dataSource = 3;

let {username, password, url, schedule, scheduleTime, dataType, dataUsername, dataPassword, dataURL, queryFile, minimum} = require('./options');

const dhis2 = new URL(url);

dhis2.username = username;
dhis2.password = password;


const baseUrl = dhis2.toString() + 'api/';

const TRACKED_ENTITY_URL = baseUrl + 'trackedEntityInstances';
const EVENT_URL = baseUrl + 'events';
const ENROLLMENT_URL = baseUrl + 'enrollments';

let running = false;

const getUniqueColumn = () => {
  const unique = mapping.programTrackedEntityAttributes.filter(a => {
    return a.trackedEntityAttribute.unique && a.column;
  });

  if (unique.length > 0) {
    return unique[0]['column']['value'];
  }

  return null;
};

const bufferToExcel = (buffer) => {
  const workbook = XLSX.read(buffer, {
    type: 'buffer',
    cellDates: true,
    cellNF: false,
    cellText: false
  });

  const sheets = workbook.SheetNames;

  if (workbook !== null && sheets.length > 0) {
    return XLSX.utils.sheet_to_json(workbook.Sheets[sheets[0]], {
      range: 0,
      dateNF: 'YYYY-MM-DD'
    });
  }
};


const downloadExcel = async (url) => {
  try {
    const response = await rq({
      uri: url,
      encoding: null
    });
    return bufferToExcel(response);
  } catch (error) {
    winston.log({
      level: 'warn',
      message: 'Something wired happened'
    });
  }
};

const readExcel = (url) => {
  const buffer = fs.readFileSync(url);
  return bufferToExcel(buffer);

};

const readAccess = (url) => {
  winston.log({
    level: 'warn',
    message: 'Access not yet supported'
  });
/*const connectionString = 'Provider=Microsoft.ACE.OLEDB.12.0;Data Source=' + url + ';Persist Security Info=False;';

db.open(connectionString,  (err) =>{
    if (err) return console.log(err);
});*/
};

const readMysql = async (url, query, params) => {
  const mysql = require('./mysql');
  const connection = new mysql(url);
  const data = await connection.query(query, params);
  await connection.close();
  return data;
};

const getUniqueIds = (data) => {
  const uniqueColumn = getUniqueColumn();
  if (uniqueColumn !== null && data && data.length > 0) {
    let foundIds = data.map(d => {
      return d[uniqueColumn];
    }).filter(c => {
      return c !== null && c !== undefined;
    });
    foundIds = _.uniq(foundIds);
    return _.chunk(foundIds, 50).map(ids => ids.join(';'));
  }
  return [];
};

const getUniqueAttribute = () => {
  const unique = mapping.programTrackedEntityAttributes.filter(a => {
    return a.trackedEntityAttribute.unique;
  });

  if (unique.length > 0) {
    return unique[0]['trackedEntityAttribute']['id']
  }

  return null;

};


const searchTrackedEntities = async (uniqueIds) => {
  let foundEntities = [];
  const uniqueAttribute = getUniqueAttribute();

  const all = uniqueIds.map(uniqueId => {
    const params = {
      paging: false,
      ouMode: 'ALL',
      filter: uniqueAttribute + ':IN:' + uniqueId,
      fields: 'trackedEntityInstance,orgUnit,attributes[attribute,value],enrollments[enrollment,program,' +
        'trackedEntityInstance,trackedEntityType,trackedEntity,enrollmentDate,incidentDate,orgUnit,events[program,trackedEntityInstance,event,' +
        'eventDate,status,completedDate,coordinate,programStage,orgUnit,dataValues[dataElement,value]]]'
    };
    return rq({
      url: TRACKED_ENTITY_URL,
      qs: params,
      json: true
    })
  });

  await Promise.all(all.map(async (response) => {
    const data = await response;
    const entities = data['trackedEntityInstances'];
    foundEntities = [...foundEntities, ...entities];
  }));

  return foundEntities
};

const removeDuplicates = (evs, stageEventFilters) => {
  if (stageEventFilters && stageEventFilters.elements && stageEventFilters.event) {
    evs = _.uniqBy(evs, v => {
      const filteredAndSame = stageEventFilters.elements.map(se => {
        const foundPrevious = _.filter(v.dataValues, {
          dataElement: se
        });
        if (foundPrevious.length > 0) {
          const exists = foundPrevious[0].value;
          return {
            exists
          };
        } else {
          return {
            exists: false
          }
        }
      });

      if (_.some(filteredAndSame, {
          'exists': false
        })) {
        return v.event;
      } else {
        return JSON.stringify([v.eventDate, filteredAndSame])
      }
    });

  } else if (stageEventFilters && stageEventFilters.elements) {

    evs = _.uniqBy(evs, v => {
      const filteredAndSame = stageEventFilters.elements.map(se => {
        const foundPrevious = _.filter(v.dataValues, {
          dataElement: se
        });
        if (foundPrevious.length > 0) {
          const exists = foundPrevious[0].value;
          return {
            exists
          };
        } else {
          return {
            exists: false
          }
        }
      });

      if (_.some(filteredAndSame, {
          'exists': false
        })) {
        return v.event;
      } else {
        return JSON.stringify([filteredAndSame])
      }
    });

  } else if (stageEventFilters && stageEventFilters.event) {
    evs = _.uniqBy(evs, v => {
      return v.eventDate;
    });
  }
  return evs;
};

const searchEvent = (enrollmentEvents, stageEventFilters, stage, e) => {
  return _.findIndex(enrollmentEvents, item => {
    if (!stageEventFilters) {
      return false
    } else if (stageEventFilters.elements && stageEventFilters.event) {
      const filteredAndSame = stageEventFilters.elements.map(se => {
        const foundPrevious = _.filter(item.dataValues, {
          dataElement: se
        });
        const foundCurrent = _.filter(e.dataValues, {
          dataElement: se
        });

        if (foundCurrent.length > 0 && foundPrevious.length > 0) {
          const exists = foundPrevious[0].value === foundCurrent[0].value;
          return {
            exists
          };
        } else {
          return {
            exists: false
          }
        }
      });
      return item.programStage === stage &&
        moment(item.eventDate).format('YYYY-MM-DD') ===
        moment(e.eventDate).format('YYYY-MM-DD')
        && _.every(filteredAndSame, 'exists');
    } else if (stageEventFilters.elements) {
      const filteredAndSame = stageEventFilters.elements.map(se => {
        const foundPrevious = _.filter(item.dataValues, {
          dataElement: se
        });
        const foundCurrent = _.filter(e.dataValues, {
          dataElement: se
        });
        if (foundCurrent.length > 0 && foundPrevious > 0) {
          return {
            exists: foundPrevious[0].value === foundCurrent[0].value
          };
        } else {
          return {
            exists: false
          }
        }
      });
      return item.programStage === stage && _.every(filteredAndSame, 'exists')
    } else if (stageEventFilters.event) {
      return item.programStage === stage &&
        moment(item.eventDate).format('YYYY-MM-DD') === moment(e.eventDate).format('YYYY-MM-DD')
    }
  });
};


const validText = (dataType, value) => {
  switch (dataType) {
    case 'TEXT':
    case 'LONG_TEXT':
      return value;
    case 'NUMBER':
      return !isNaN(value);
    case 'EMAIL':
      const re = /\S+@\S+\.\S+/;
      return re.test(String(value).toLowerCase());
    case 'BOOLEAN':
      return value === false || value === true;
    case 'TRUE_ONLY':
      return value === true;
    case 'PERCENTAGE':
      return value >= 0 && value <= 100;
    case 'INTEGER':
      return !isNaN(value) && !isNaN(parseInt(value, 10));
    case 'DATE':
    case 'DATETIME':
    case 'TIME':
      return moment(value).isValid();
    case 'UNIT_INTERVAL':
      return value >= 0 && value <= 1;
    case 'INTEGER_NEGATIVE':
      return Number.isInteger(value) && value < 0;
    case 'NEGATIVE_INTEGER':
      return Number.isInteger(value) && value < 0;
    case 'INTEGER_ZERO_OR_POSITIVE':
    case 'AGE':
      return Number.isInteger(value) && value >= 0;
    default:
      return true
  }
};

const validateValue = (dataType, value, optionSet) => {
  if (optionSet) {
    const options = optionSet.options.map(o => {
      return {
        code: o.code,
        value: o.value
      }
    });
    const coded = _.find(options, o => {
      return value + '' === o.code + '' || value + '' === o.value + '';
    });
    if (coded !== undefined && coded !== null) {
      return coded.code;
    }
  } else if (validText(dataType, value)) {
    if (dataType === 'DATETIME') {
      return moment(value).format('YYYY-MM-DDTHH:mm:ss')
    } else if (dataType === 'DATE') {
      return moment(value).format('YYYY-MM-DD')
    } else if (dataType === 'TIME') {
      return moment(value).format('HH:mm')
    }
    return value;
  }
  return null;
};

const searchOrgUnit = val => {
  const orgUnitStrategy = mapping.orgUnitStrategy;
  const organisationUnits = mapping.organisationUnits;
  switch (orgUnitStrategy.value) {
    case 'uid':
      return _.find(organisationUnits, {
        id: val
      });
    case 'code':
      return _.find(organisationUnits, {
        code: val
      });
    case 'name':
      return _.find(organisationUnits, {
        name: val
      });
    case 'auto':
      const s1 = _.find(organisationUnits, {
        id: val
      });
      const s2 = _.find(organisationUnits, {
        code: val
      });
      const s3 = _.find(organisationUnits, {
        name: val
      });
      if (s1 !== undefined) {
        return s1;
      } else if (s2 !== undefined) {
        return s2;
      } else if (s3 !== undefined) {
        return s3;
      } else {
        return undefined;
      }
    default:
      return undefined;
  }
};

const fileExists = (file) => {
  return fs.existsSync(file);
};

const searchedInstances = (trackedEntityInstances) => {
  const unique = getUniqueAttribute();
  const entities = trackedEntityInstances.map(e => {
    const uniqueAttribute = _.find(e.attributes, {
      attribute: unique
    });
    const val = uniqueAttribute ? uniqueAttribute['value'] : null;
    return {
      ...e,
      ..._.fromPairs([[unique, val]])
    }
  });
  return _.groupBy(entities, unique);
};

const isTracker = () => {
  return mapping.programType === 'WITH_REGISTRATION';
};


const processData = (data, foundEntities) => {

  let eventsUpdate = [];
  let trackedEntityInstancesUpdate = [];

  let newEvents = [];
  let newEnrollments = [];
  let newTrackedEntityInstances = [];

  let duplicates = [];
  let conflicts = [];
  let errors = [];

  const uniqueColumn = getUniqueColumn();

  const programStages = mapping.programStages;
  const eventDateColumn = mapping.eventDateColumn;
  const programTrackedEntityAttributes = mapping.programTrackedEntityAttributes;
  const enrollmentDateColumn = mapping.enrollmentDateColumn;
  const incidentDateColumn = mapping.incidentDateColumn;

  const searched = searchedInstances(foundEntities);
  if (uniqueColumn) {
    data = data.filter(d => {
      return d[uniqueColumn] !== null && d[uniqueColumn] !== undefined;
    });
    let clients = _.groupBy(data, uniqueColumn);
    let newClients = [];
    _.forOwn(clients, (data, client) => {
      const previous = searched[client] || [];
      newClients = [...newClients, {
        client,
        data,
        previous
      }];
    });
    data = newClients;
  } else if (data && data.length > 0) {
    data = data.map((data, i) => {
      return {
        data: [data],
        client: i + 1,
        previous: []
      };
    });
  }

  if (data && data.length > 0) {
    data.forEach(client => {
      let events = [];
      let allAttributes = [];
      let currentData = client.data;
      let enrollmentDates = [];
      let orgUnits = [];
      let identifierElements = {};
      currentData.forEach(d => {
        programStages.forEach(stage => {
          let dataValues = [];
          let eventDate;
          if ((mapping.createNewEvents || mapping.updateEvents) && mapping.dataSource === 2) {
            eventDate = d[eventDateColumn.value];
          } else if (mapping.createNewEvents || mapping.updateEvents) {
            const date = moment(d[eventDateColumn.value], 'YYYY-MM-DD');
            if (date.isValid()) {
              eventDate = date.format('YYYY-MM-DD');
            }
          }

          const mapped = stage.programStageDataElements.filter(e => {
            return e.column && e.column.value
          });

          identifierElements[stage.id] = {
            elements: mapped.filter(e => {
              return e.dataElement.identifiesEvent;
            }).map(e => e.dataElement.id),
            event: stage.eventDateIdentifiesEvent
          };
          // Coordinates
          let coordinate = null;
          if (stage.latitudeColumn && stage.longitudeColumn) {
            coordinate = {
              latitude: d[stage.latitudeColumn.value],
              longitude: d[stage.longitudeColumn.value]
            };
          }
          if (eventDate && mapped.length > 0) {
            mapped.forEach(e => {
              const value = d[e.column.value];
              const type = e.dataElement.valueType;
              const optionsSet = e.dataElement.optionSet;
              const validatedValue = validateValue(type, value, optionsSet);
              const row = client.client;
              const column = e.column.value;
              if (value !== '' && validatedValue !== null) {
                dataValues = [...dataValues, {
                  dataElement: e.dataElement.id,
                  value: validatedValue
                }];
              } else if (value !== undefined) {
                const error = optionsSet === null ? 'Invalid value ' + value + ' for value type ' + type :
                  'Invalid value: ' + value + ', expected: ' + _.map(optionsSet.options, o => {
                    return o.code
                  }).join(',');

                const message = [error, 'row:' + row, 'column:' + column].join(' ');
                winston.log('info', message);
              }
            });

            let event = {
              dataValues,
              eventDate,
              programStage: stage.id,
              program: mapping.id,
              event: generateUid()
            };
            if (coordinate) {
              event = {
                ...event,
                coordinate
              }
            }

            if (stage.completeEvents) {
              event = {
                ...event, ...{
                      status: 'COMPLETED',
                      completedDate: event['eventDate']
                }
              }
            }

            events = [...events, event];
          }
        });
        const mappedAttributes = programTrackedEntityAttributes.filter(a => {
          return a.column && a.column.value
        });

        let attributes = [];

        mappedAttributes.forEach(a => {
          const value = d[a.column.value];
          const type = a.valueType;
          const optionsSet = a.trackedEntityAttribute.optionSet;
          const validatedValue = validateValue(type, value, optionsSet);

          if (value !== '' && validatedValue !== null) {
            attributes = [...attributes, {
              attribute: a.trackedEntityAttribute.id,
              value: validatedValue
            }]
          } else if (value !== undefined) {
            winston.log('info', 'value was empty on column: ' + column + ', row: ' + row);
          }

        });

        if (attributes.length > 0) {
          allAttributes = [...allAttributes, attributes];
        }

        if (isTracker() && enrollmentDateColumn && incidentDateColumn) {
          const enrollmentDate = moment(d[enrollmentDateColumn.value], 'YYYY-MM-DD');
          const incidentDate = moment(d[incidentDateColumn.value], 'YYYY-MM-DD');

          if (enrollmentDate.isValid() && incidentDate.isValid()) {
            enrollmentDates = [...enrollmentDates, {
              enrollmentDate: enrollmentDate.format('YYYY-MM-DD'),
              incidentDate: incidentDate.format('YYYY-MM-DD')
            }]
          }
        }

        if (mapping.orgUnitColumn !== '') {
          orgUnits = [...orgUnits, d[mapping.orgUnitColumn.value]]
        }
      });
      let groupedEvents = _.groupBy(events, 'programStage');
      if (client.previous.length > 1) {
        duplicates = [...duplicates, client.previous]
      } else if (client.previous.length === 1) {
        client.previous.forEach(p => {
          let enrollments = p['enrollments'];
          if (mapping.updateEntities) {
            const nAttributes = _.differenceWith(allAttributes[0], p['attributes'], _.isEqual);
            if (nAttributes.length > 0) {
              const mergedAttributes = _.unionBy(allAttributes[0], p['attributes'], 'attribute');
              let tei;
              if (mapping.trackedEntityType && mapping.trackedEntityType.id) {
                tei = {
                  ..._.pick(p, ['orgUnit', 'trackedEntityInstance', 'trackedEntityType']),
                  attributes: mergedAttributes
                };
              } else if (mapping.trackedEntity) {
                tei = {
                  ..._.pick(p, ['orgUnit', 'trackedEntityInstance', 'trackedEntity']),
                  attributes: mergedAttributes
                };
              }
              trackedEntityInstancesUpdate = [...trackedEntityInstancesUpdate, tei];
            }
          }
          events = events.map(e => {
            return {
              ...e,
              trackedEntityInstance: p['trackedEntityInstance'],
              orgUnit: p['orgUnit']
            }
          });

          groupedEvents = _.groupBy(events, 'programStage');
          const enrollmentIndex = _.findIndex(enrollments, {
            program: mapping.id
          });
          if (enrollmentIndex === -1 && mapping.createNewEnrollments && enrollmentDates.length > 0) {
            let enroll = {
              program: id,
              orgUnit: p['orgUnit'],
              trackedEntityInstance: p['trackedEntityInstance'],
              ...enrollmentDates[0]
            };
            newEnrollments = [...newEnrollments, enroll];
            if (mapping.createNewEvents) {
              _.forOwn(groupedEvents, (evs, stage) => {
                const stageEventFilters = identifierElements[stage];
                const stageInfo = _.find(programStages, {
                  id: stage
                });
                const {repeatable} = stageInfo;

                evs = removeDuplicates(evs, stageEventFilters);

                if (!repeatable) {
                  const ev = _.maxBy(evs, 'eventDate');
                  if (ev.dataValues.length > 0) {
                    newEvents = [...newEvents, ev];
                  }
                } else {
                  newEvents = [...newEvents, ...evs];
                }
              });
            } else {
              console.log('Ignoring not creating new events');
            }
            enrollments = [...enrollments, enroll];
            p = {
              ...p,
              enrollments
            }
          } else if (enrollmentIndex === -1 && enrollmentDates.length === 0) {
            console.log('Ignoring new enrollments');
          } else if (enrollmentIndex !== -1) {
            let enrollment = enrollments[enrollmentIndex];
            let enrollmentEvents = enrollment['events'];
            _.forOwn(groupedEvents, (evs, stage) => {
              const stageInfo = _.find(programStages, {
                id: stage
              });
              const {repeatable} = stageInfo;

              const stageEventFilters = identifierElements[stage];

              evs = removeDuplicates(evs, stageEventFilters);

              if (repeatable) {
                evs.forEach(e => {
                  const eventIndex = searchEvent(enrollmentEvents, stageEventFilters, stage, e);
                  if (eventIndex !== -1 && mapping.updateEvents) {
                    const stageEvent = enrollmentEvents[eventIndex];
                    const merged = _.unionBy(e['dataValues'], stageEvent['dataValues'], 'dataElement');
                    const differingElements = _.differenceWith(e['dataValues'], stageEvent['dataValues'], _.isEqual);
                    if (merged.length > 0 && differingElements.length > 0) {
                      const mergedEvent = {
                        ...stageEvent,
                        dataValues: merged
                      };
                      eventsUpdate = [...eventsUpdate, mergedEvent];
                    }
                  } else if (eventIndex === -1 && mapping.createNewEvents) {
                    newEvents = [...newEvents, e];
                  }
                });
              } else {
                let foundEvent = _.find(enrollmentEvents, {
                  programStage: stage
                });
                let max = _.maxBy(evs, 'eventDate');
                if (foundEvent && mapping.updateEvents) {
                  const merged = _.unionBy(max['dataValues'], foundEvent['dataValues'], 'dataElement');
                  const differingElements = _.differenceWith(max['dataValues'], foundEvent['dataValues'], _.isEqual);
                  if (merged.length > 0 && differingElements.length > 0) {
                    const mergedEvent = {
                      ...foundEvent,
                      dataValues: merged
                    };
                    eventsUpdate = [...eventsUpdate, mergedEvent];
                  }
                } else if (!foundEvent && mapping.createNewEvents) {
                  newEvents = [...newEvents, max];
                }
              }
            });
          }
        });
      } else {
        orgUnits = _.uniq(orgUnits);
        let orgUnit;
        if (orgUnits.length > 1) {
          errors = [...errors, {
            error: 'Entity belongs to more than one organisation unit',
            row: client.client
          }]
        } else if (orgUnits.length === 1) {
          orgUnit = searchOrgUnit(orgUnits[0]);
          if (orgUnit) {
            if (enrollmentDates.length > 0 && isTracker && mapping.createNewEnrollments && mapping.createEntities) {
              const trackedEntityInstance = generateUid();
              let tei = {
                orgUnit: orgUnit.id,
                attributes: allAttributes[0],
                trackedEntityInstance
              };

              if (mapping.trackedEntityType && mapping.trackedEntityType.id) {
                tei = {
                  ...tei,
                  trackedEntityType: mapping.trackedEntityType.id
                }
              } else if (mapping.trackedEntity && mapping.trackedEntity.id) {
                tei = {
                  ...tei,
                  trackedEntity: mapping.trackedEntity.id
                }
              }

              newTrackedEntityInstances = [...newTrackedEntityInstances, tei];

              let enrollment = {
                orgUnit: orgUnit.id,
                program: mapping.id,
                trackedEntityInstance,
                ...enrollmentDates[0],
                enrollment: generateUid()
              };

              if (mapping.createNewEvents) {
                _.forOwn(groupedEvents, (evs, stage) => {
                  const stageEventFilters = identifierElements[stage];
                  const stageInfo = _.find(programStages, {
                    id: stage
                  });
                  const {repeatable} = stageInfo;
                  evs = evs.map(e => {
                    return {
                      ...e,
                      orgUnit: orgUnit.id,
                      event: generateUid(),
                      trackedEntityInstance
                    }
                  });

                  evs = removeDuplicates(evs, stageEventFilters);

                  if (!repeatable) {
                    newEvents = [...newEvents, _.maxBy(evs, 'eventDate')];
                  } else {
                    newEvents = [...newEvents, ...evs]
                  }
                });
              }
              newEnrollments = [...newEnrollments, enrollment];
            } else if (!isTracker() && mapping.createNewEvents) {
              events = events.map(e => {
                return {
                  ...e,
                  orgUnit: orgUnit.id
                }
              });
              newEvents = [...newEvents, ...events];
            }
          } else {
            winston.log('warn', 'Organisation unit ' + orgUnits[0] + ' not found using strategy ' + mapping.orgUnitStrategy.value);
          }
        } else if (orgUnits.length === 0) {
          winston.log('warn', 'Organisation unit missing for entity: ' + client.client);
        }
      }
    });
  }

  return {
    newTrackedEntityInstances,
    newEnrollments,
    newEvents,
    trackedEntityInstancesUpdate,
    eventsUpdate,
    conflicts,
    duplicates,
    errors
  }
};

const insertTrackedEntityInstance = (data) => {

  const options = {
    method: 'POST',
    uri: TRACKED_ENTITY_URL,
    body: data,
    json: true
  };

  return rq(options);
};

const processResponse = (response, type) => {
  // console.log(JSON.stringify(response, null, 2));
  // responses.forEach(response => {
  if (response['httpStatusCode'] === 200) {
    const {importSummaries} = response['response'];
    importSummaries.forEach(importSummary => {
      const {importCount, reference} = importSummary;

      winston.log('info', type + ' with id, ' + reference + ' imported: ' + importCount.imported + ', updated: ' + importCount.updated + ', deleted: ' + importCount.deleted);
    });
  } else if (response['httpStatusCode'] === 409) {
    _.forEach(response['response']['importSummaries'], (s) => {
      _.forEach(s['conflicts'], (conflict) => {
        winston.log('warn', type + ' conflict found, object: ' + conflict.object + ', message: ' + conflict.value);
      });
    });
  } else if (response['httpStatusCode'] === 500) {
    winston.log('error', JSON.stringify(response, null, 2));
  }
// });
};


const insertEnrollment = (data) => {
  const options = {
    method: 'POST',
    uri: ENROLLMENT_URL,
    body: data,
    json: true
  };

  return rq(options);
};


const insertEvent = (data) => {
  const options = {
    method: 'POST',
    uri: EVENT_URL,
    body: data,
    json: true
  };

  return rq(options);
};


const pullMapping = async (minimum) => {
  // try {
  let data = null;
  if (dataType && dataType !== '' && dataURL && dataURL !== '') {
    switch (dataType) {
      case 'mysql':
        if (queryFile && fileExists(queryFile)) {
          const sql = fs.readFileSync(queryFile).toString();
          data = await readMysql(dataURL, sql, [minimum, moment(new Date()).format('YYYY-MM-DD HH:mm:ss')]);

        } else {
          winston.log('error', 'Mysql query file not found');
        }
        break;
      case 'excel':
        if (dataURL && fileExists(dataURL)) {
          data = readExcel(dataURL);
        } else {
          winston.log('error', 'Specified excel file can not be found');
        }
        break;
      case 'excel-download':
        const downloadUrl = new url(dataURL);
        if (dataUsername && dataPassword) {
          downloadUrl.username = dataUsername;
          downloadUrl.password = dataPassword;
        }
        const reachable = await isReachable(downloadUrl);
        if (reachable) {
          data = await downloadExcel(downloadUrl.toString());
        } else {
          winston.log('error', 'Specified url not reachable');
        }
        break;
      case 'access':
        data = readAccess(dataURL);
        break;
      default:
        winston.log('error', 'Unknown database', {
          value: dataType,
          expected: ['mysql', 'excel', 'access']
        });
    }

  } else if (mapping.url !== '') {
    let params = {};
    if (mapping.dateFilter !== '' && mapping.dateEndFilter !== '') {
      if (minimum !== null) {
        params = {
          ...params, ..._.fromPairs([[mapping.dateFilter, minimum],
                [mapping.dateEndFilter, moment(new Date()).format('YYYY-MM-DD HH:mm:ss')]])
        };
      }
    }

    try {
      // const reachable = await isReachable(mapping.url);
      // if (reachable) {
      data = await rq({
        url: mapping.url,
        qs: params,
        json: true
      });

      data = data.map(d => {
        return _.pickBy(d, _.identity);
      });
    // } else {
    //     winston.log('error', 'Url specified in the mapping not reachable');
    // }
    } catch (e) {
      winston.log('error', e.toString());
    }

  } else {
    winston.log('warn', 'Url not found in the mapping or database and connection url not specified');
  }


  const uniqueIds = getUniqueIds(data);
  const foundEntities = await searchTrackedEntities(uniqueIds);
  const processed = processData(data, foundEntities);

  console.log(processed)

  // Inserting

  const {newTrackedEntityInstances, newEnrollments, newEvents, trackedEntityInstancesUpdate, eventsUpdate} = processed;

  const allInstances = [...newTrackedEntityInstances, ...trackedEntityInstancesUpdate];
  const allEvents = [...newEvents, ...eventsUpdate];

  try {
    if (allInstances.length > 0) {
      const instancesResults = await insertTrackedEntityInstance({
        trackedEntityInstances: allInstances
      });
      processResponse(instancesResults, 'Tracked entity instance');
    }
  } catch (e) {
    processResponse(e, 'Tracked entity instance');
  }

  try {
    if (newEnrollments.length > 0) {
      const enrollmentsResults = await insertEnrollment({
        enrollments: newEnrollments
      });
      processResponse(enrollmentsResults, 'Enrollment');
    }
  } catch (e) {
    processResponse(e, 'Enrollment');
  }

  try {
    if (allEvents.length > 0) {
      const eventsResults = await insertEvent({
        events: allEvents
      });
      processResponse(eventsResults, 'Event');
    }
  } catch (e) {
    console(JSON.stringify(e));
    processResponse(e, 'Event');
  }
/* } catch (e) {
     winston.log('error', JSON.stringify(e));
 }*/
};
// pullMapping(args, minimum);
if (schedule) {
  setInterval(async () => {
    // const reachable = await isReachable(url);
    // if (reachable) {
    if (running) {
      return;
    } else {
      running = true;
      await pullMapping(minimum);
      minimum = moment(new Date()).format('YYYY-MM-DD HH:mm:ss')
      running = false;
    }

  // } else {
  //     winston.log('error', 'DHIS2 not reachable verify your DHIS2 server is reachable and that your dhis2 url is valid');
  // }
  }, scheduleTime * 1000);
} else {
  // isReachable(url).then(async reachable => {
  //     if (reachable) {
  //         await pullMapping(minimum);
  //         winston.log('info', 'Importing complete');
  //     } else {
  //         winston.log('error', 'DHIS2 not reachable verify your DHIS2 server is reachable and that your dhis2 url is valid');
  //     }
  // });

  (async () => {
    await pullMapping(minimum);
    winston.log('info', 'Importing complete');
  })();

}