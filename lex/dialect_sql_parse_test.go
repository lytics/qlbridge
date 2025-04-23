package lex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lytics/qlbridge/rel"
)

func parseSqlTest(t *testing.T, sql string) {
	sqlRequest, err := rel.ParseSql(sql)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, sqlRequest, "Must parse: %s  \n\t%v", sql, err)
	if _, ok := sqlRequest.(*rel.SqlSelect); ok {
		_, err2 := rel.ParseSqlSelect(sql)
		assert.Equal(t, nil, err2)
	}
}
func parseSqlError(t *testing.T, sql string) {
	_, err := rel.ParseSql(sql)
	assert.NotEqual(t, nil, err, "Must error on parse: %s", sql)
}

func TestSqlParser(t *testing.T) {

	parseSqlTest(t, `## this is a comment
		SELECT a FROM x;`)

	parseSqlError(t, `SELECT a FROM x LIMIT 1 NOTAWORD;`)

	parseSqlError(t, `SELECT a, tolower(b) AS b INTO newtable FROM FROM WHERE a != "hello";`)
	parseSqlTest(t, `
		SELECT a.language, a.template, Count(*) AS count
		FROM
			(Select Distinct language, template FROM content WHERE language != "en" OFFSET 1) AS a
			Left Join users AS b
				On b.language = a.language AND b.template = b.template
		GROUP BY a.language, a.template`)
	parseSqlTest(t, `
			SELECT a FROM x WHERE a IN (select ax FROM z);
		`)

	// CREATE
	parseSqlTest(t, `CREATE CONTINUOUSVIEW viewx AS SELECT a FROM tbl;`)
	parseSqlError(t, `CREATE FAKEITEM viewx;`)
	parseSqlTest(t, `CREATE OR REPLACE VIEW viewx
		AS SELECT a, b FROM mydb.tbl
		WITH stuff = "hello";`)
	parseSqlTest(t, `CREATE TABLE articles
			--comment-here
			(
			 ID int(11) NOT NULL AUTO_INCREMENT,
			 Email char(150) NOT NULL DEFAULT '' UNIQUE COMMENT 'this-is-comment',
			 stuff varchar(150),
			 profile text,
			 PRIMARY KEY (ID),
			 visitct BIGINT,
			 CONSTRAINT emails_fk FOREIGN KEY (Email) REFERENCES Emails (Email)
		   ) ENGINE=InnoDB AUTO_INCREMENT=4080 DEFAULT CHARSET=utf8;`)

	parseSqlTest(t, `CREATE INDEX IF NOT EXISTS my_index ON my_table (col1, col2) WITH { "key": "value" };`)

	// DROP
	parseSqlTest(t, `DROP CONTINUOUSVIEW viewx WITH stuff = "hello";`)
}
