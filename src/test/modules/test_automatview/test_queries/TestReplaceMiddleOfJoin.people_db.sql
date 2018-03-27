SET automatview.training_sample_count = 2;

select employee.id, person_id, manager.id, employee_id from employee join manager on employee.id = employee_id;
select employee.id, person_id, manager.id, employee_id from employee join manager on employee.id = employee_id;

select name, age, employee.id, executive.id from person join employee on person.id = person_id
 join manager on employee.id = employee_id join executive on manager.id = manager_id;
