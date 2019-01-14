const organisms = {
  'New Patient UID': '1234',
  'department': 'A',
  'Organism1': 'A',
  'Organism2': 'A',
  'Organism3': 'A',
  'Organism4': 'A',
  'Other Organism': 'A'
};

for (var i = 1; i <= 19; i++) {
  organisms['Organism1' + ' Antibiotics' + i] = 'A';
  organisms['Organism1' + 'Antibiotics' + i + 'Result' + i] = 'A';

  organisms['Organism2' + ' Antibiotics' + i] = 'A';
  organisms['Organism2' + 'Antibiotics' + i + 'Result' + i] = 'A';

  organisms['Organism3' + ' Antibiotics' + i] = 'A';
  organisms['Organism3' + 'Antibiotics' + i + 'Result' + i] = 'A';

  organisms['Organism4' + ' Antibiotics' + i] = 'A';
  organisms['Organism4' + 'Antibiotics' + i + 'Result' + i] = 'A';

  organisms['OrganismT' + ' Antibiotics' + i] = 'A';
  organisms['OrganismT' + 'Antibiotics' + i + 'Result' + i] = 'A';

}

console.log(JSON.stringify([organisms]))