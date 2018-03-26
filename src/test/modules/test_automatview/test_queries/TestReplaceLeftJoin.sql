select person.id, age from person join employee on person.id = person_id;
--select person.id, name, age, person_id, employee.id from person join employee on person.id = person_id;
select person.id, name, age, person_id, employee.id from person join employee on person.id = person_id;
select person_id, name, age, manager.id from person join employee on person.id = person_id join manager on employee.id = employee_id;

/*
 * Expected:
 * 
 * select Matview.employee_person_id, MatView.person_name, MatView.person_age, manager.id
 *  from MatView join manager on MatView.employee_id = manager.employee_id
 */

/*
 * Actual (as of 3/9/18):
 * 
 * select Matview.employee_person_id, MatView.person_name, MatView.person_age, manager.id
 *  from MatView join manager on MatView.employee_id = manager.employee_id
 */