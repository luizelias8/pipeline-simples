DROP DATABASE IF EXISTS pipeline;

CREATE DATABASE pipeline;

USE pipeline;

DROP TABLE IF EXISTS clima;

CREATE TABLE clima (
	id INT AUTO_INCREMENT PRIMARY KEY,
    cidade VARCHAR(100),
    temperatura FLOAT,
    umidade INT,
    descricao VARCHAR(255),
    data_hora DATETIME
);

SELECT * FROM clima;