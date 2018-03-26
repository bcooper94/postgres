select employee.id, person_id, manager.id, employee_id from employee join manager on employee.id = employee_id;
--select employee.id, person_id, employee_id from employee join manager on employee.id = employee_id;
select employee.id, person_id, manager.id, employee_id from employee join manager on employee.id = employee_id;
--select employee.id, manager.id, person_id, employee_id from employee join manager on employee.id = employee_id;

select name, age, employee.id, executive.id from person join employee on person.id = person_id
 join manager on employee.id = employee_id join executive on manager.id = manager_id;

/*
 * Expected: select person.name, person.age, MatView.employee_id, executive.id
 * 	from person join MatView on person.id = MatView.employee_person_id
 * 	join executive on MatView.manager_id = executive.manager_id;
 */

/*
 * Actual (3/18/18):
 * SELECT person.name, person.age, Matview.employee_id, executive.id
 * FROM person JOIN Matview ON person.id = Matview.employee_person_id
 * JOIN executive ON Matview.manager_id = executive.manager_id
 */