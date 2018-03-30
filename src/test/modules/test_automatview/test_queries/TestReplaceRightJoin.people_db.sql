SET automatview.training_sample_count = 2;

select employee.id, person_id, employee_id from employee join manager on employee.id = employee_id;
select employee.id, person_id from employee join manager on employee.id = employee_id;

select person_id, name, age from person join employee on person.id = person_id join manager on employee.id = employee_id;