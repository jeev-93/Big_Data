/*
Lab 0 report Olayemi Morrison (olamo208) and Greeshma Jeev Koothuparambil (greko370)
*/

/* All non code should be within SQL-comments like this */ 


/*
Drop all user created tables that have been created when solving the lab
*/

DROP TABLE IF EXISTS custom_table CASCADE;


/* Have the source scripts in the file so it is easy to recreate!*/

SOURCE company_schema.sql;
SOURCE company_data.sql;

1) List all employees, i.e. all tuples in the jbemployee relation.
select * from jbemployee;

# 1) List the name of all departments in alphabetical order. Note: by “name” we mean the name attribute for all tuples in the jbdept relation.
select name from jbdept order by name;

# 3) What parts are not in store, i.e. qoh = 0? (qoh = Quantity On Hand)
select * from jbparts where qoh = 0;

# 4) Which employees have a salary between 9000 (included) and 10000 (included)?
select * from jbemployee where salary between 9000 and 10000;

# 5) What was the age of each employee when they started working (startyear)?
select name,startyear - birthyear age from jbemployee;

# 6) Which employees have a last name ending with “son”?
select * from jbemployee where name like '%son,%';

# 7) Which items (note items, not parts) have been delivered by a supplier called Fisher-Price? Formulate this query using a subquery in the where-clause.
select * from jbitem where supplier = (select id from jbsupplier where name = 'Fisher-Price');

# 8) Formulate the same query as above, but without a subquery.
select i.* from jbitem i, jbsupplier s where (i.supplier = s.id && s.name = 'Fisher-Price');

# 9) Show all cities that have suppliers located in them. Formulate this query using a subquery in the where-clause.
select * from jbcity where id in (select city from jbsupplier);

# 10) What is the name and color of the parts that are heavier than a card reader? Formulate this query using a subquery in the where-clause. (The SQL query must not contain the weight as a constant.)
select name, color from jbparts where weight > (select weight from jbparts where name = 'card reader');

# 11) Formulate the same query as above, but without a subquery. (The query must not contain the weight as a constant.)
select jbparts.name, jbparts.color from jbparts join jbparts as card_reader on jbparts.weight > card_reader.weight where card_reader.name = 'card reader';

# 12) What is the average weight of black parts?
select avg(weight) from jbparts where color = 'black';

# 13) What is the total weight of all parts that each supplier in Massachusetts (“Mass”) has delivered? Retrieve the name and the total weight for each of these suppliers. Do not forget to take the quantity of delivered parts into account. Note that one row should be returned for each supplier
select s.name,sum(s.quan * p.weight) as 'total weight' from (
	select * from jbsupply s join (
		select id,name from jbsupplier where city in (
			select id from jbcity where state = 'mass'
		)
	)as slr on s.supplier = slr.id
)as s join jbparts as p on s.part = p.id  group by supplier
;

# 14)  Create a new relation (a table), with the same attributes as the table items using the CREATE TABLE syntax where you define every attribute explicitly (i.e. not as a copy of another table). Then fill the table with all items that cost less than the average price for items. Remember to define primary and foreign keys in your table!
create table items(
    id int unsigned,
    name varchar(20),
    dept int,
    price int unsigned,
    qoh int unsigned,
    supplier int,
    constraint pk_item primary key(id),
	constraint fk_deliver_supplier foreign key (supplier) references jbsupplier(id),
    constraint fk_onshelf_dept foreign key (dept) references jbdept(id)
);
select * from jbitem where price < (select avg(price) from jbitem);
insert into items(id, name, dept, price, qoh, supplier)

select * from jbitem where price < (select avg(price) from jbitem);

select * from items;