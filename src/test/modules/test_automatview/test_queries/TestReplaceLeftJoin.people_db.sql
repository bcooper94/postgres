SET automatview.training_sample_count = 2;

select person.id, age from person join employee on person.id = person_id;
select person.id, name, age, person_id, employee.id from person join employee on person.id = person_id;
select person_id, name, age, manager.id from person join employee on person.id = person_id join manager on employee.id = employee_id;