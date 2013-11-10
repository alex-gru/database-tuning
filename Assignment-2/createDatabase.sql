CREATE TABLE Employee(
	ssnum INT,
	name VARCHAR(10),
	manager VARCHAR(13),
	dept VARCHAR(10),
	salary INT,
	numfriends INT,
	PRIMARY KEY (ssnum, name)
);

CREATE UNIQUE INDEX index_ssnum_e ON Employee(ssnum);
CREATE UNIQUE INDEX index_name_e ON Employee(name); 
CREATE INDEX index_dept_e ON Employee(dept); 

CREATE TABLE Student(
	ssnum INT,
	name VARCHAR(10),
	course VARCHAR(12),
	grade INT,
	PRIMARY KEY (ssnum, name)
);

CREATE UNIQUE INDEX index_ssnum_s ON Employee(ssnum);
CREATE UNIQUE INDEX index_name_s ON Employee(name); 

CREATE TABLE Techdept(
	dept VARCHAR(10),
	manager VARCHAR(13),
	location VARCHAR(14),
	grade INT,
	PRIMARY KEY (dept)
);

CREATE UNIQUE INDEX index_dept_t ON Techdept(dept);
