SET automatview.training_sample_count = 2;

select person.id, age from person join employee on person.id = person_id;
select person.id, age, employee.id, person_id from person join employee on person.id = person_id;

select person_id, name, age from person join employee on person.id = person_id join manager on employee.id = employee_id;

/**
 * Expect no matches because person.name is missing from MatView.
 */