create database if not exists retail;
use retail;
CREATE TABLE `retail`.`users` (
		  `user_id` INT NOT NULL AUTO_INCREMENT,
		  `user_name` VARCHAR(45) NOT NULL,
		  `department` VARCHAR(45) NOT NULL,
		  PRIMARY KEY (`user_id`));
		  
insert into `retail`.`users`(`user_name`,`department`) values ('prashant khunt','Big Data Development');
insert into `retail`.`users`(`user_name`,`department`) values ('Jayesh Patel','HR Department');
insert into `retail`.`users`(`user_name`,`department`) values ('Pratik S','Java');
insert into `retail`.`users`(`user_name`,`department`) values ('Prakash P','Web Development');
insert into `retail`.`users`(`user_name`,`department`) values ('Jagdish K','Designing');