select employee.id, person_id, employee_id from employee join manager on employee.id = employee_id;
select employee.id, person_id from employee join manager on employee.id = employee_id;

select person_id, name, age from person join employee on person.id = person_id join manager on employee.id = employee_id;

/* Expected:
 * 
 * select MatView.employee_person_id, person.name, person.age
 * 	from person join MatView on person.id = MatView.employee_person_id
 */

/* Actual (3/18/18):
 * SELECT Matview.employee_person_id, person.name, person.age
 * FROM person JOIN Matview ON person.id = Matview.employee_person_id
 */