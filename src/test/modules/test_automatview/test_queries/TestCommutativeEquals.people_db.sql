SET automatview.training_sample_count = 3;

SELECT name, age FROM person JOIN employee ON person_id = person.id JOIN manager ON employee.id = employee_id
    JOIN executive ON manager_id = manager.id;

SELECT name, age FROM person JOIN employee ON person.id = person_id JOIN manager ON employee.id = employee_id
    JOIN executive ON manager_id = manager.id;

SELECT name, age FROM person JOIN employee ON person.id = person_id JOIN manager ON employee_id = employee.id
    JOIN executive ON manager_id = manager.id;

/* Matview query */
SELECT name, age FROM person JOIN employee ON person_id = person.id JOIN manager ON employee_id = employee.id
    JOIN executive ON manager.id = manager_id;