let eventsUpdate = [];
  let trackedEntityInstancesUpdate = [];

  let newEvents = [];
  let newEnrollments = [];
  let newTrackedEntityInstances = [];

  let duplicates = [];

  const uniqueColumn = getUniqueColumn();

  const programStages = mapping.programStages;
  const eventDateColumn = mapping.eventDateColumn;
  const programTrackedEntityAttributes = mapping.programTrackedEntityAttributes;
  const enrollmentDateColumn = mapping.enrollmentDateColumn;
  const incidentDateColumn = mapping.incidentDateColumn;

  const searched = searchedInstances(foundEntities);
  if (uniqueColumn) {
    // Ignore data without unique column
    data = data.filter(d => {
      return d[uniqueColumn] && d[uniqueColumn] !== null && d[uniqueColumn] !== undefined
    })
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
          let eventDate = null;
          const momentDate = moment(d[eventDateColumn.value]);
          if (momentDate.isValid()) {
            eventDate = momentDate.format('YYYY-MM-DD');
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
              const type = e.dataElement['valueType'];
              const row = client.client;
              const column = e.column.value;
              const optionsSet = e.dataElement['optionSet'];
              const validatedValue = validateValue(type, value, optionsSet);
              if (value !== '' && validatedValue !== null) {
                dataValues = [...dataValues, {
                  dataElement: e.dataElement.id,
                  value: validatedValue
                }];
              } else if (value !== undefined && value !== null) {

                const error = optionsSet === null ? 'Invalid value ' + value + ' for value type ' + type :
                  'Invalid value: ' + value + ', expected: ' + _.map(optionsSet.options, o => {
                    return o.code
                  }).join(',');

                const message = [error, 'row:' + row, 'column:' + column].join(' ');

              // winston.log('warn', message);
              } else if (value === '') {
                winston.log('info', 'value was empty on column: ' + column + ', row: ' + row);
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
                      completedDate: eventDate
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
          const row = client.client;
          const column = a.column.value;
          const value = d[a.column.value];
          const type = a['valueType'];
          const optionsSet = a.trackedEntityAttribute.optionSet;
          const validatedValue = validateValue(type, value, optionsSet);
          if (value !== '' && validatedValue !== null) {
            attributes = [...attributes, {
              attribute: a.trackedEntityAttribute.id,
              value: validatedValue
            }]
          } else if (value === '') {
            winston.log('info', 'value was empty on column: ' + column + ', row: ' + row);
          } else {

            const error = optionsSet === null ? 'Invalid value ' + value + ' for value type ' + type :
              'Invalid value: ' + value + ', expected: ' + _.map(optionsSet.options, o => {
                return o.code
              }).join(',');


            const message = [error, 'row:' + row, 'column:' + column].join(' ');

          // winston.log('warn', message);
          }

        });

        if (attributes.length > 0) {
          allAttributes = [...allAttributes, attributes];
        }

        if (isTracker() && enrollmentDateColumn && incidentDateColumn) {
          const enrollmentDate = moment(d[enrollmentDateColumn.value]);
          const incidentDate = moment(d[incidentDateColumn.value]);

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
          const nAttributes = _.differenceBy(allAttributes[0], p['attributes'], _.isEqual);
          let enrollments = p['enrollments'];
          if (nAttributes.length > 0) {
            const mergedAttributes = _.unionBy(allAttributes[0], p['attributes'], 'attribute');
            let tei = {
              ..._.pick(p, ['orgUnit', 'trackedEntityInstance', 'trackedEntityType']),
              attributes: mergedAttributes
            };
            trackedEntityInstancesUpdate = [...trackedEntityInstancesUpdate, tei];
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
          if (enrollmentIndex === -1 && enrollmentDates.length > 0) {
            let enroll = {
              program: mapping.id,
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
              winston.log({
                level: 'warn',
                message: 'Ignoring not creating new events'
              });
            }
            enrollments = [...enrollments, enroll];
            p = {
              ...p,
              enrollments
            }
          } else if (enrollmentIndex === -1 && enrollmentDates.length === 0) {
            winston.log('warn', 'Ignoring new enrollments');
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

              // console.log(evs);

              if (repeatable) {
                evs.forEach(e => {
                  const eventIndex = searchEvent(enrollmentEvents, stageEventFilters, stage, e);
                  // console.log(eventIndex, client.client, e);
                  if (eventIndex !== -1) {
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
                  } else {
                    newEvents = [...newEvents, e];
                  }
                });
              } else {
                let foundEvent = _.find(enrollmentEvents, {
                  programStage: stage
                });
                let max = _.maxBy(evs, 'eventDate');
                if (foundEvent) {
                  const merged = _.unionBy(max['dataValues'], foundEvent['dataValues'], 'dataElement');
                  const differingElements = _.differenceWith(max['dataValues'], foundEvent['dataValues'], _.isEqual);
                  if (merged.length > 0 && differingElements.length > 0) {
                    const mergedEvent = {
                      ...foundEvent,
                      dataValues: merged
                    };
                    eventsUpdate = [...eventsUpdate, mergedEvent];
                  }
                } else {
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
          winston.log('warn', 'Entity belongs to more than one organisation unit for entity: ' + JSON.stringify(client));
        } else if (orgUnits.length === 1) {
          orgUnit = searchOrgUnit(orgUnits[0], mapping);
          if (orgUnit) {
            if (enrollmentDates.length > 0 && isTracker && mapping.createNewEnrollments) {
              const trackedEntityInstance = generateUid();
              let tei = {
                orgUnit: orgUnit.id,
                attributes: allAttributes[0],
                trackedEntityInstance,
                trackedEntityType: mapping.trackedEntityType.id,
              };
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
            // winston.log('warn', 'Organisation unit ' + orgUnits[0] + ' not found using strategy ' + mapping.orgUnitStrategy.value);
          }
        } else if (orgUnits.length === 0) {
          winston.log('warn', 'Organisation unit missing for entity: ' + client.client);
        }
      }
    });
  }

  if (newTrackedEntityInstances.length) {
    winston.log('info', newTrackedEntityInstances.length + ' new tracked entity instances found');
  }
  if (trackedEntityInstancesUpdate.length) {
    winston.log('info', trackedEntityInstancesUpdate.length + ' tracked entity instances updates found');
  }
  if (newEnrollments.length) {
    winston.log('info', newEnrollments.length + ' new enrollments found');
  }
  if (newEvents.length) {
    console.log(JSON.stringify(newEvents, null, 2));
    winston.log('info', newEvents.length + ' new events found');
  }
  if (eventsUpdate.length) {
    winston.log('info', eventsUpdate.length + ' new event updates found');
  }

  return {
    newTrackedEntityInstances,
    newEnrollments,
    newEvents,
    trackedEntityInstancesUpdate,
    eventsUpdate
  }