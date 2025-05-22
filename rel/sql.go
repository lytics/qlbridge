// Package rel are the AST Structures and Parsers
// for the SQL, FilterQL, and Expression dialects.
package rel

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"strings"

	u "github.com/araddon/gou"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/lex"
	"github.com/lytics/qlbridge/value"
)

var (
	// Ensure SqlSelect and cousins etc are SqlStatements
	_ SqlStatement = (*SqlSelect)(nil)
	_ SqlStatement = (*SqlInsert)(nil)
	_ SqlStatement = (*SqlUpsert)(nil)
	_ SqlStatement = (*SqlUpdate)(nil)
	_ SqlStatement = (*SqlDelete)(nil)
	_ SqlStatement = (*SqlShow)(nil)
	_ SqlStatement = (*SqlDescribe)(nil)
	_ SqlStatement = (*SqlCommand)(nil)
	_ SqlStatement = (*SqlInto)(nil)

	// sub-query statements
	_ SqlSourceStatement = (*SqlSource)(nil)

	// Statements with Columns
	_ ColumnsStatement = (*SqlSelect)(nil)

	// A select * columns
	starCols Columns
)

func init() {
	starCols = make(Columns, 1)
	starCols[0] = NewColumnFromToken(lex.Token{T: lex.TokenStar, V: "*"})
}

type (
	// ColumnsStatement is a statement interface for those statements that
	// have columns that need to be added during parse.
	ColumnsStatement interface {
		AddColumn(col Column) error
	}
	// SqlStatement interface, to define the sql statement
	// Select, Insert, Update, Delete, Command, Show, Describe etc
	SqlStatement interface {
		// string representation of Node, AST parseable back to itself
		String() string
		// Write out this statement in a specific dialect
		WriteDialect(w expr.DialectWriter)
		// SQL keyword (select, insert, etc)
		Keyword() lex.TokenType
	}

	// SqlSourceStatement interface, to define the subselect/join-types
	// Join, SubSelect, From
	SqlSourceStatement interface {
		// string representation of Node, AST parseable back to itself
		String() string
		// Write out this statement in a specific dialect
		WriteDialect(w expr.DialectWriter)
		// SQL Keyword for this statement
		Keyword() lex.TokenType
	}
)

type (
	// PreparedStatement Prepared/Aliased SQL statement
	PreparedStatement struct {
		Alias     string
		Statement SqlStatement
	}
	// SqlSelect SQL Select statement
	SqlSelect struct {
		Db        string       // If provided a use "dbname"
		Raw       string       // full original raw statement
		Star      bool         // for select * from ...
		Distinct  bool         // Distinct flag?
		Columns   Columns      // An array (ordered) list of columns
		From      []*SqlSource // From, Join
		Into      *SqlInto     // Into "table"
		Where     *SqlWhere    // Expr Node, or *SqlSelect
		Having    expr.Node    // Filter results
		GroupBy   Columns
		OrderBy   Columns
		Limit     int
		Offset    int
		Alias     string       // Non-Standard sql, alias/name of sql another way of expression Prepared Statement
		With      u.JsonHelper // Non-Standard SQL for properties/config info, similar to Cassandra with, purse json
		proj      *Projection  // Projected fields
		isAgg     bool         // is this an aggregate query?  has group-by, or aggregate selector expressions (count, cardinality etc)
		finalized bool         // have we already finalized, ie formalized left/right aliases
		schemaqry bool         // is this a schema qry?  ie select @@max_packet etc

		fingerprintid int64
	}
	// SqlSource is a table name, sub-query, or join as used in
	// SELECT <columns> FROM <SQLSOURCE>
	//  - SELECT .. FROM table_name
	//  - SELECT .. from (select a,b,c from tableb)
	//  - SELECT .. FROM tablex INNER JOIN ...
	SqlSource struct {
		final       bool               // has this been finalized?
		alias       string             // either the short table name or full
		cols        map[string]*Column // Un-aliased columns, ie "x.y" -> "y"
		colIndex    map[string]int     // Key(alias) to index in []driver.Value positions
		joinNodes   []expr.Node        // x.y = q.y AND x.z = q.z  --- []Node{Identity{x},Identity{z}}
		Source      *SqlSelect         // Sql Select Source query, written by Rewrite
		Raw         string             // Raw Partial Query
		Name        string             // From Name (optional, empty if join, subselect)
		Alias       string             // From name aliased
		Schema      string             //  FROM `schema`.`table`
		Op          lex.TokenType      // In, =, ON
		LeftOrRight lex.TokenType      // Left, Right
		JoinType    lex.TokenType      // INNER, OUTER
		JoinExpr    expr.Node          // Join expression       x.y = q.y
		SubQuery    *SqlSelect         // optional, Join/SubSelect statement

		// Plan Hints, move to a dedicated planner
		Seekable bool
	}
	// SqlWhere WHERE is select stmt, or set of expressions
	// - WHERE x in (select name from q)
	// - WHERE x = y
	// - WHERE x = y AND z = q
	// - WHERE tolower(x) IN (select name from q)
	SqlWhere struct {
		// Either Op + Source exists
		Op     lex.TokenType // (In|=|ON)  for Select Clauses operators
		Source *SqlSelect    // IN (SELECT a,b,c from z)

		// OR expr but not both
		Expr expr.Node // x = y AND q > 5
	}
	// SqlInsert SQL Insert Statement
	SqlInsert struct {
		kw      lex.TokenType    // Insert, Replace
		Table   string           // table name
		Columns Columns          // Column Names
		Rows    [][]*ValueColumn // Values to insert
		Select  *SqlSelect       //
	}
	// SqlUpsert SQL Upsert Statement
	SqlUpsert struct {
		Columns Columns
		Rows    [][]*ValueColumn
		Values  map[string]*ValueColumn
		Where   *SqlWhere
		Table   string
	}
	// SqlUpdate SQL Update Statement
	SqlUpdate struct {
		Values map[string]*ValueColumn
		Where  *SqlWhere
		Table  string
	}
	// SqlDelete SQL Delete Statement
	SqlDelete struct {
		Table string
		Where *SqlWhere
		Limit int
	}
	// SqlShow SQL SHOW Statement
	SqlShow struct {
		Raw        string // full raw statement
		Db         string // Database/Schema name
		Full       bool   // SHOW FULL TABLE FROM
		Scope      string // {FULL, GLOBAL, SESSION}
		ShowType   string // object type, [tables, columns, etc]
		From       string // `table`   or `schema`.`table`
		Identity   string // `table`   or `schema`.`table`
		Create     bool
		CreateWhat string
		Where      expr.Node
		Like       expr.Node
	}
	// SQL Describe statement
	SqlDescribe struct {
		Raw      string    // full original raw statement
		Identity string    // Describe
		Tok      lex.Token // Explain, Describe, Desc
		Stmt     SqlStatement
	}
	// SqlInto   INTO statement   (select a,b,c from y INTO z)
	SqlInto struct {
		Table string
	}
	// SqlCommand is admin command such as "SET", "USE"
	SqlCommand struct {
		kw       lex.TokenType  // SET or USE
		Columns  CommandColumns // can have multiple columns in command
		Identity string         //
		Value    expr.Node      //
	}
	// SqlCreate SQL CREATE statement
	SqlCreate struct {
		Raw         string       // full original raw statement
		Identity    string       // identity of table, view, etc
		Parent      string       // identity of table, view, etc
		Tok         lex.Token    // CREATE [INDEX|TABLE,VIEW,CONTINUOUSVIEW,TRIGGER] etc
		OrReplace   bool         // OR REPLACE
		IfNotExists bool         // IF NOT EXISTS
		Cols        []*DdlColumn // columns
		Engine      map[string]any
		With        u.JsonHelper
		Select      *SqlSelect
	}
	// SqlDrop SQL DROP statement
	SqlDrop struct {
		Raw      string    // full original raw statement
		Identity string    // identity of table, view, etc
		Temp     bool      // Temp?
		Tok      lex.Token // DROP [TEMP] [TABLE,VIEW,CONTINUOUSVIEW,TRIGGER] etc
		With     u.JsonHelper
	}
	// SqlAlter SQL ALTER statement
	SqlAlter struct {
		Raw      string       // full original raw statement
		Identity string       // identity to alter
		Tok      lex.Token    // ALTER [TABLE,VIEW,CONTINUOUSVIEW,TRIGGER] etc
		Cols     []*DdlColumn // columns
	}
	// Columns List of Columns in SELECT [columns]
	Columns []*Column
	// Column represents the Column as expressed in a [SELECT]
	// expression
	Column struct {
		sourceQuoteByte byte      // quote mark?   [ or ` etc
		asQuoteByte     byte      // quote mark   [ or `
		originalAs      string    // original as string
		left            string    // users.col_name   = "users"
		right           string    // users.first_name = "first_name"
		isLiteral       bool      // is this a literal column?
		ParentIndex     int       // slice idx position in parent query cols
		Index           int       // slice idx position in original query cols
		SourceIndex     int       // slice idx position in source []driver.Value
		SourceField     string    // field name of underlying field
		SourceOriginal  string    // field name of underlying field without the "left.right" parse
		As              string    // As field, auto-populate the Field Name if exists
		Comment         string    // optional in-line comments
		Order           string    // (ASC | DESC)
		Star            bool      // *
		Agg             bool      // aggregate function column?   count(*), avg(x) etc
		Expr            expr.Node // Expression, optional, often Identity.Node
		Guard           expr.Node // column If guard, non-standard sql column guard
	}
	// ValueColumn List of Value columns in INSERT into TABLE (colnames) VALUES (valuecolumns)
	ValueColumn struct {
		Value value.Value
		Expr  expr.Node
	}
	// DdlColumn represents the Data Definition Column
	DdlColumn struct {
		Kw            lex.TokenType // initial keyword (identity for normal, constraint, primary)
		Null          bool          // Do we support NULL?
		AutoIncrement bool          // auto increment
		IndexType     string        // index_type
		IndexCols     []string      // index_col_name
		RefTable      string        // refererence table
		RefCols       []string      // ref cols
		Default       expr.Node     // Default value
		DataType      string        // data type
		DataTypeSize  int           // Data Type Size:    varchar(2000)
		DataTypeArgs  []expr.Node   // data type args
		Key           lex.TokenType // UNIQUE | PRIMARY
		Name          string        // name
		Comment       string        // optional in-line comments
		Expr          expr.Node     // Expression, optional, often Identity.Node but could be composite key
	}
	// ResultColumns List of ResultColumns used to describe projection response columns
	ResultColumns []*ResultColumn
	// Result Column used in projection
	ResultColumn struct {
		Final  bool            // Is this part of final projection (ie, response)
		Name   string          // Original path/name for query field
		ColPos int             // Ordinal position in sql (or partial sql) statement
		Col    *Column         // the original sql column
		Star   bool            // Was this a select * ??
		As     string          // aliased
		Type   value.ValueType // Data Type
	}
	// Projection describes the results to expect from sql statement
	// ie the ResultColumns for a result-set
	Projection struct {
		Distinct bool
		Final    bool // Is this a Final Projection? or intermiediate?
		colNames map[string]struct{}
		Columns  ResultColumns
	}
	// CommandColumns SQL commands such as:
	//     set autocommit
	//     SET @@local.sort_buffer_size=10000;
	//     USE myschema;
	CommandColumns []*CommandColumn
	// CommandColumn is single column such as "autocommit"
	CommandColumn struct {
		Expr expr.Node // column expression
		Name string    // Original path/name for command field
	}
)

func NewSqlDialect() expr.DialectWriter {
	return expr.NewKeywordDialect(SqlKeywords)
}
func NewProjection() *Projection {
	return &Projection{Columns: make(ResultColumns, 0), colNames: make(map[string]struct{})}
}
func NewResultColumn(as string, ordinal int, col *Column, valtype value.ValueType) *ResultColumn {
	rc := ResultColumn{Name: as, As: as, ColPos: ordinal, Col: col, Type: valtype}
	if col != nil {
		rc.Name = col.SourceField
	}
	return &rc
}
func NewSqlSelect() *SqlSelect {
	req := &SqlSelect{}
	req.Columns = make(Columns, 0)
	return req
}
func NewSqlInsert() *SqlInsert {
	req := &SqlInsert{}
	req.Columns = make(Columns, 0)
	return req
}
func NewSqlUpdate() *SqlUpdate {
	req := &SqlUpdate{}
	return req
}
func NewSqlUpsert() *SqlUpsert {
	req := &SqlUpsert{}
	return req
}
func NewSqlDelete() *SqlDelete {
	return &SqlDelete{}
}
func NewPreparedStatement() *PreparedStatement {
	return &PreparedStatement{}
}
func NewSqlCreate() *SqlCreate {
	req := &SqlCreate{}
	return req
}
func NewSqlDrop() *SqlDrop {
	req := &SqlDrop{}
	return req
}
func NewSqlInto(table string) *SqlInto {
	return &SqlInto{Table: table}
}
func NewSqlSource(table string) *SqlSource {
	return &SqlSource{Name: table}
}
func NewSqlWhere(where expr.Node) *SqlWhere {
	return &SqlWhere{Expr: where}
}
func NewColumnFromToken(tok lex.Token) *Column {
	_, r, _ := expr.LeftRight(tok.V)
	v := tok.V
	if tok.Quote != 0 {
		//v = expr.IdentityMaybeQuote(tok.Quote, v)
	}
	return &Column{
		As:              tok.V,
		sourceQuoteByte: tok.Quote,
		asQuoteByte:     tok.Quote,
		SourceField:     r,
		SourceOriginal:  v,
	}
}
func NewColumnValue(tok lex.Token) *Column {
	return &Column{
		sourceQuoteByte: tok.Quote,
		asQuoteByte:     tok.Quote,
	}
}
func NewColumn(col string) *Column {
	return &Column{
		As:          col,
		SourceField: col,
		Expr:        &expr.IdentityNode{Text: col},
	}
}

// The source column name
func (m *ResultColumn) SourceName() string {
	if m.Col != nil && m.Col.SourceField != "" {
		return m.Col.SourceField
	}
	return m.Name
}
func (m *ResultColumn) Equal(s *ResultColumn) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Final != s.Final {
		return false
	}
	if m.Name != s.Name {
		return false
	}
	if m.ColPos != s.ColPos {
		return false
	}
	if m.Star != s.Star {
		return false
	}
	if m.As != s.As {
		return false
	}
	if m.Type != s.Type {
		return false
	}
	if m.Col != nil && !m.Col.Equal(s.Col) {
		//u.Warnf("Not Equal?   %T  vs %T", m.Col, s.Col)
		//u.Warnf("t!=t:   \n\t%#v\n\t%#v", m.Col, s.Col)
		return false
	}
	return true
}
func (m *Projection) AddColumnShort(colName string, vt value.ValueType) {
	//colName = strings.ToLower(colName)
	// if _, exists := m.colNames[colName]; exists {
	// 	return
	// }
	//u.Infof("adding column %s to %v", colName, m.colNames)
	//m.colNames[colName] = struct{}{}
	m.Columns = append(m.Columns, NewResultColumn(colName, len(m.Columns), nil, vt))
}
func (m *Projection) AddColumn(col *Column, vt value.ValueType) {
	//colName := strings.ToLower(col.As)
	// if _, exists := m.colNames[colName]; exists {
	// 	return
	// }
	//m.colNames[colName] = struct{}{}
	m.Columns = append(m.Columns, NewResultColumn(col.As, len(m.Columns), col, vt))
}
func (m *Projection) Equal(s *Projection) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Distinct != s.Distinct {
		return false
	}
	if len(m.colNames) != len(s.colNames) {
		return false
	}
	for name := range m.colNames {
		_, hasSameName := s.colNames[name]
		if !hasSameName {
			return false
		}
	}
	if len(m.Columns) != len(s.Columns) {
		return false
	}
	for i, c := range m.Columns {
		if !c.Equal(s.Columns[i]) {
			//u.Warnf("Not Equal?   %T  vs %T", c, s.Columns[i])
			//u.Warnf("t!=t:   \n\t%#v \n\t!= %#v", c, s.Columns[i])
			return false
		}
	}
	return true
}
func (m *Columns) WriteDialect(w expr.DialectWriter) {
	colCt := len(*m)
	if colCt == 1 {
		(*m)[0].WriteDialect(w)
		return
	} else if colCt == 0 {
		return
	}
	for i, col := range *m {
		if i != 0 {
			io.WriteString(w, ", ")
		}
		col.WriteDialect(w)
	}
}
func (m *Columns) String() string {
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *Columns) FieldNames() []string {
	names := make([]string, len(*m))
	for i, col := range *m {
		names[i] = col.Key()
	}
	return names
}
func (m *Columns) UnAliasedFieldNames() []string {
	names := make([]string, len(*m))
	for i, col := range *m {
		_, right, _ := col.LeftRight()
		names[i] = right
	}
	return names
}
func (m *Columns) AliasedFieldNames() []string {
	names := make([]string, len(*m))
	for i, col := range *m {
		names[i] = col.As
	}
	return names
}
func (m *Columns) ByName(name string) (*Column, bool) {
	for _, col := range *m {
		//u.Debugf("col.SourceField='%s' key()='%s' As='%s' ", col.SourceField, col.Key(), col.As)
		if col.SourceField == name || col.Key() == name {
			return col, true
		}
	}
	return nil, false
}
func (m *Columns) ByAs(as string) (*Column, bool) {
	for _, col := range *m {
		if col.As == as {
			return col, true
		}
	}
	return nil, false
}
func (m Columns) Equal(cols Columns) bool {
	if len(m) != len(cols) {
		return false
	}
	for i, c := range m {
		if !c.Equal(cols[i]) {
			return false
		}
	}
	return true
}

func (m *Column) Key() string {
	if m.left != "" {
		return m.right
	}
	return m.As
}
func (m *Column) String() string {
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *Column) WriteDialect(w expr.DialectWriter) {
	if m.Star {
		io.WriteString(w, "*")
		return
	}
	exprStr := ""
	if m.Expr != nil {
		start := w.Len()
		m.Expr.WriteDialect(w)
		if w.Len() > start {
			exprStr = w.String()[start:]
		}
	}

	if m.asQuoteByte != 0 && m.originalAs != "" {
		io.WriteString(w, " AS ")
		w.WriteIdentity(m.As)
	} else if m.originalAs != "" && exprStr != m.originalAs {
		io.WriteString(w, " AS ")
		w.WriteIdentity(m.originalAs)
	} else if m.Expr == nil {
		w.WriteIdentity(m.As)
	}
	if m.Guard != nil {
		io.WriteString(w, " IF ")
		m.Guard.WriteDialect(w)
	}
	if m.Order != "" {
		io.WriteString(w, " ")
		io.WriteString(w, m.Order)
	}
}

// Is this a select count(*) column
func (m *Column) CountStar() bool {
	if m.Expr == nil {
		return false
	}
	if fn, ok := m.Expr.(*expr.FuncNode); ok {
		if len(fn.Args) != 1 {
			return false
		}
		return strings.ToLower(fn.Name) == "count" && fn.Args[0].String() == `*`
	}
	return false
}
func (m *Column) InFinalProjection() bool {
	return m.ParentIndex >= 0
}
func (m *Column) IsLiteral() bool {
	if m.Expr == nil {
		return false
	}
	switch n := m.Expr.(type) {
	case *expr.FuncNode:
		// count(*)
		// now()
		// tolower(field_name)
		idents := expr.FindAllIdentityField(n)
		if len(idents) > 0 {
			return false
		}
		if m.Agg && m.CountStar() {
			return true
		}
	case *expr.IdentityNode:
		if n.IsBooleanIdentity() {
			return true
		}
		return false
	case *expr.StringNode, *expr.NumberNode, *expr.ValueNode:
		return true
	default:
		u.Warnf("Unknown Node column type? %T", n)
	}
	return false
}

func (m *Column) IsLiteralOrFunc() bool {
	if m.Expr == nil {
		return false
	}
	switch n := m.Expr.(type) {
	case *expr.FuncNode:
		// count(*)
		// now()
		// tolower(field_name)
		return true
	case *expr.IdentityNode:
		// What about NULL?
		if n.IsBooleanIdentity() {
			return true
		}
		return false
	case *expr.StringNode, *expr.NumberNode, *expr.ValueNode:
		return true
	}
	return false
}

func (m *Column) Asc() bool {
	return strings.ToLower(m.Order) == "asc"
}
func (m *Column) Equal(c *Column) bool {
	if m == nil && c == nil {
		return true
	}
	if m == nil && c != nil {
		return false
	}
	if m != nil && c == nil {
		return false
	}
	if m.sourceQuoteByte != c.sourceQuoteByte {
		return false
	}
	if m.asQuoteByte != c.asQuoteByte {
		return false
	}
	if m.originalAs != c.originalAs {
		return false
	}
	if m.left != c.left {
		return false
	}
	if m.right != c.right {
		return false
	}
	if m.ParentIndex != c.ParentIndex {
		return false
	}
	if m.Index != c.Index {
		return false
	}
	if m.SourceIndex != c.SourceIndex {
		return false
	}
	if m.SourceField != c.SourceField {
		return false
	}
	if m.As != c.As {
		return false
	}
	if m.Comment != c.Comment {
		return false
	}
	if m.Order != c.Order {
		return false
	}
	if m.Star != c.Star {
		return false
	}
	if m.Expr != nil {
		if !m.Expr.Equal(c.Expr) {
			return false
		}
	}
	if m.Guard != nil {
		if !m.Guard.Equal(c.Guard) {
			return false
		}
	}
	return true
}

// CopyRewrite Create a new copy of this column for rewrite purposes removing alias
func (m *Column) CopyRewrite(alias string) *Column {
	left, right, _ := m.LeftRight()
	newCol := m.Copy()
	//u.Warnf("in rewrite:  Alias:'%s'  '%s'.'%s'  sourcefield:'%v'", alias, left, right, m.SourceField)
	if left == alias {
		newCol.SourceField = right
		newCol.right = right
	}
	if newCol.Expr != nil {
		_, right, _ := expr.LeftRight(newCol.Expr.String())
		if right == m.SourceField {
			newCol.Expr = &expr.IdentityNode{Text: right}
		}
	}
	return newCol
}

// Copy - deep copy, shared nothing
func (m *Column) Copy() *Column {
	return &Column{
		sourceQuoteByte: m.sourceQuoteByte,
		asQuoteByte:     m.asQuoteByte,
		originalAs:      m.originalAs,
		ParentIndex:     m.ParentIndex,
		Index:           m.Index,
		SourceField:     m.SourceField,
		As:              m.right,
		Comment:         m.Comment,
		Order:           m.Order,
		Star:            m.Star,
		Expr:            m.Expr,
		Guard:           m.Guard,
	}
}

// Return left, right values if is of form   `table.column` and
// also return true/false for if it even has left/right
func (m *Column) LeftRight() (string, string, bool) {
	if m.right == "" {
		m.left, m.right, _ = expr.LeftRight(m.As)
	}
	return m.left, m.right, m.left != ""
}

func (m *PreparedStatement) Keyword() lex.TokenType { return lex.TokenPrepare }
func (m *PreparedStatement) String() string {
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *PreparedStatement) WriteDialect(w expr.DialectWriter) {
	io.WriteString(w, "PREPARE ")
	w.WriteIdentity(m.Alias)
	io.WriteString(w, " FROM ")
	m.Statement.WriteDialect(w)
}

func (m *SqlSelect) Keyword() lex.TokenType { return lex.TokenSelect }
func (m *SqlSelect) SystemQry() bool        { return len(m.From) == 0 && m.schemaqry }
func (m *SqlSelect) SetSystemQry()          { m.schemaqry = true }
func (m *SqlSelect) IsLiteral() bool        { return len(m.From) == 0 }
func (m *SqlSelect) Equal(ss SqlStatement) bool {
	s, ok := ss.(*SqlSelect)
	if !ok {
		return false
	}
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Db != s.Db {
		return false
	}
	if m.Raw != s.Raw {
		return false
	}
	if m.Star != s.Star {
		return false
	}
	if m.Distinct != s.Distinct {
		return false
	}
	if m.Limit != s.Limit {
		return false
	}
	if m.Offset != s.Offset {
		return false
	}
	if m.Alias != s.Alias {
		return false
	}
	if m.isAgg != s.isAgg {
		return false
	}
	if m.finalized != s.finalized {
		return false
	}
	if m.schemaqry != s.schemaqry {
		return false
	}
	if !m.Into.Equal(s.Into) {
		return false
	}
	if m.Where != nil && !m.Where.Equal(s.Where) {
		return false
	}
	if m.Having != nil && !m.Having.Equal(s.Having) {
		return false
	}

	if len(m.Columns) != len(s.Columns) {
		return false
	}
	for i, c := range m.Columns {
		if !c.Equal(s.Columns[i]) {
			return false
		}
	}
	if len(m.From) != len(s.From) {
		return false
	}
	for i, c := range m.From {
		if !c.Equal(s.From[i]) {
			return false
		}
	}
	if len(m.GroupBy) != len(s.GroupBy) {
		return false
	}
	for i, c := range m.GroupBy {
		if !c.Equal(s.GroupBy[i]) {
			return false
		}
	}
	if len(m.OrderBy) != len(s.OrderBy) {
		return false
	}
	for i, c := range m.OrderBy {
		if !c.Equal(s.OrderBy[i]) {
			return false
		}
	}
	if !m.proj.Equal(s.proj) {
		return false
	}
	return true
}

// SqlSelectFromPb take a protobuf select struct and conver to SqlSelect
func (m *SqlSelect) IsAggQuery() bool {
	if m.isAgg || len(m.GroupBy) > 0 {
		return true
	}
	return false
}
func (m *SqlSelect) String() string {
	w := NewSqlDialect()
	m.writeDialectDepth(0, w)
	return w.String()
}
func (m *SqlSelect) writeDialectDepth(depth int, w expr.DialectWriter) {

	io.WriteString(w, "SELECT ")
	if m.Distinct {
		io.WriteString(w, "DISTINCT ")
	}
	m.Columns.WriteDialect(w)
	if m.Into != nil {
		io.WriteString(w, " INTO ")
		w.WriteIdentity(m.Into.Table)
	}
	if m.From != nil {
		io.WriteString(w, " FROM")
		for i, from := range m.From {
			if i == 0 {
				io.WriteString(w, " ")
			} else {
				if from.SubQuery != nil {
					io.WriteString(w, "\n")
					io.WriteString(w, strings.Repeat("\t", depth+1))
				} else {
					io.WriteString(w, "\n")
					io.WriteString(w, strings.Repeat("\t", depth+1))
				}
			}
			from.writeDialectDepth(depth+1, w)
		}
	}
	if m.Where != nil {
		io.WriteString(w, " WHERE ")
		m.Where.writeDialectDepth(depth, w)
	}
	if len(m.GroupBy) > 0 {
		io.WriteString(w, " GROUP BY ")
		m.GroupBy.WriteDialect(w)
	}
	if m.Having != nil {
		io.WriteString(w, " HAVING ")
		m.Having.WriteDialect(w)
	}
	if len(m.OrderBy) > 0 {
		io.WriteString(w, " ORDER BY ")
		m.OrderBy.WriteDialect(w)
	}
	if m.Limit > 0 {
		io.WriteString(w, fmt.Sprintf(" LIMIT %d", m.Limit))
	}
	if m.Offset > 0 {
		io.WriteString(w, fmt.Sprintf(" OFFSET %d", m.Offset))
	}
}
func (m *SqlSelect) FingerPrintID() int64 {
	if m.fingerprintid == 0 {
		h := fnv.New64()
		w := expr.NewFingerPrinter()
		m.WriteDialect(w)
		h.Write([]byte(w.String()))
		m.fingerprintid = int64(h.Sum64())
	}
	return m.fingerprintid
}
func (m *SqlSelect) WriteDialect(w expr.DialectWriter) {
	m.writeDialectDepth(0, w)
}

// Finalize this Query plan by preparing sub-sources
//
//	ie we need to rewrite some things into sub-statements
//	- we need to share the join expression across sources
func (m *SqlSelect) Finalize() error {
	if m.finalized {
		return nil
	}
	m.finalized = true
	if len(m.From) == 0 {
		return nil
	}
	for _, from := range m.From {
		from.Finalize()
	}

	return nil
}

func (m *SqlSelect) UnAliasedColumns() map[string]*Column {
	cols := make(map[string]*Column, len(m.Columns))
	for _, col := range m.Columns {
		_, right, _ := col.LeftRight()
		cols[right] = col
	}
	return cols
}
func (m *SqlSelect) AliasedColumns() map[string]*Column {
	cols := make(map[string]*Column, len(m.Columns))
	for _, col := range m.Columns {
		//u.Debugf("aliasing: key():%-15q  As:%-15q   %-15q", col.Key(), col.As, col.String())
		cols[col.Key()] = col
	}
	return cols
}
func (m *SqlSelect) ColIndexes() map[string]int {
	cols := make(map[string]int, len(m.Columns))
	for i, col := range m.Columns {
		//u.Debugf("aliasing: key():%-15q  As:%-15q   %-15q", col.Key(), col.As, col.String())
		cols[col.Key()] = i
	}
	return cols
}

func (m *SqlSelect) AddColumn(colArg Column) error {
	col := &colArg
	col.Index = len(m.Columns)
	m.Columns = append(m.Columns, col)
	if col.Star {
		m.Star = true
	}

	if col.As == "" && col.Expr == nil && !col.Star {
		return fmt.Errorf("Must have *, Expression, or Identity to be a column %+v", col)
	}
	if col.Agg && !m.isAgg {
		m.isAgg = true
	}
	return nil
}

// Is this a select count(*) FROM ...   query?
func (m *SqlSelect) CountStar() bool {
	if len(m.Columns) != 1 {
		return false
	}
	col := m.Columns[0]
	if col.Expr == nil {
		return false
	}
	if f, ok := col.Expr.(*expr.FuncNode); ok {
		if strings.ToLower(f.Name) != "count" {
			return false
		}
		if len(f.Args) == 1 && f.Args[0].String() == "*" {
			return true
		}
	}
	return false
}

// Rewrite take current SqlSelect statement and re-write it
func (m *SqlSelect) Rewrite() {
	for _, f := range m.From {
		f.Rewrite(m)
	}
}

// RewriteAsRawSelect We are removing Column Aliases "user_id as uid"
// as well as functions - used when we are going to defer projection, aggs
func (m *SqlSelect) RewriteAsRawSelect() {
	RewriteSelect(m)
}

func (m *SqlSource) IsLiteral() bool        { return len(m.Name) == 0 }
func (m *SqlSource) Keyword() lex.TokenType { return m.Op }
func (m *SqlSource) SourceName() string {
	if m == nil {
		return ""
	}
	if m.SubQuery != nil {
		if len(m.SubQuery.From) == 1 {
			return m.SubQuery.From[0].Name
		}
		u.Warnf("could not find source name bc SubQuery had %d sources", len(m.SubQuery.From))
		return ""
	}
	_, right, hasLeft := expr.LeftRight(m.Name)
	if hasLeft {
		return right
	}
	return right
}
func (m *SqlSource) String() string {
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *SqlSource) WriteDialect(w expr.DialectWriter) {
	m.writeDialectDepth(0, w)
}
func (m *SqlSource) writeDialectDepth(depth int, w expr.DialectWriter) {

	if int(m.Op) == 0 && int(m.LeftOrRight) == 0 && int(m.JoinType) == 0 {
		if m.Alias != "" {
			w.WriteIdentity(m.Name)
			io.WriteString(w, " AS ")
			w.WriteIdentity(m.Alias)
			return
		}
		if m.Schema == "" {
			w.WriteIdentity(m.Name)
		} else {
			w.WriteIdentity(m.Schema)
			io.WriteString(w, ".")
			w.WriteIdentity(m.Name)
		}
		return
	}

	//   Jointype                Op
	//  INNER JOIN orders AS o 	ON
	if int(m.JoinType) != 0 {
		io.WriteString(w, strings.ToTitle(m.JoinType.String())) // inner/outer
		io.WriteString(w, " ")
	}
	io.WriteString(w, "JOIN ")

	if m.SubQuery != nil {
		io.WriteString(w, "(\n"+strings.Repeat("\t", depth+1))
		m.SubQuery.writeDialectDepth(depth+1, w)
		io.WriteString(w, "\n"+strings.Repeat("\t", depth)+")")
	} else {
		if m.Schema == "" {
			w.WriteIdentity(m.Name)
		} else {
			w.WriteIdentity(m.Schema)
			io.WriteString(w, ".")
			w.WriteIdentity(m.Name)
		}

	}
	if m.Alias != "" {
		io.WriteString(w, " AS ")
		w.WriteIdentity(m.Alias)
	}

	io.WriteString(w, " ")
	io.WriteString(w, strings.ToTitle(m.Op.String()))

	if m.JoinExpr != nil {
		w.Write([]byte{' '})
		m.JoinExpr.WriteDialect(w)
	}
}

func (m *SqlSource) BuildColIndex(colNames []string) error {
	if len(m.colIndex) == 0 {
		m.colIndex = make(map[string]int, len(colNames))
	}
	if len(colNames) == 0 {
		u.LogTraceDf(u.WARN, 10, "No columns?")
	}
	starDelta := 0 // how many columns were added due to *
	for _, col := range m.Source.Columns {
		if col.Star {
			starStart := len(m.colIndex)
			for colIdx := range colNames {
				m.colIndex[col.Key()] = colIdx + starStart
			}
			starDelta = len(colNames)
		} else {
			found := false
			for colIdx, colName := range colNames {
				_, colName, _ = expr.LeftRight(colName)
				//u.Debugf("col.Key():%v  sourceField:%v  colName:%v", col.Key(), col.SourceField, colName)
				if colName == col.Key() || col.SourceField == colName { //&&
					//u.Debugf("build col:  idx=%d  key=%-15q as=%-15q col=%-15s sourcidx:%d", len(m.colIndex), col.Key(), col.As, col.String(), colIdx)
					m.colIndex[col.Key()] = colIdx + starDelta
					col.SourceIndex = colIdx + starDelta
					found = true
					break
				}
			}
			if !found && !col.IsLiteralOrFunc() {
				return fmt.Errorf("Missing Column in source: %q", col.String())
			}
		}
	}
	return nil
}

// Rewrite this Source to act as a stand-alone query to backend
// @parentStmt = the parent statement that this a partial source to
func (m *SqlSource) Rewrite(parentStmt *SqlSelect) *SqlSelect {
	return RewriteSqlSource(m, parentStmt)
}

func (m *SqlSource) findFromAliases() (string, string) {
	from1, from2 := m.alias, ""
	if m.JoinExpr != nil {
		switch nt := m.JoinExpr.(type) {
		case *expr.BinaryNode:
			if in, ok := nt.Args[0].(*expr.IdentityNode); ok {
				if left, _, ok := in.LeftRight(); ok {
					from1 = left
				}
			}
			if in, ok := nt.Args[1].(*expr.IdentityNode); ok {
				if left, _, ok := in.LeftRight(); ok {
					from2 = left
				}
			}
		default:
			u.Warnf("%T node types are not suppored yet for join rewrite", m.JoinExpr)
		}
	}
	return from1, from2
}

// Get a list of Un-Aliased Columns, ie columns with column
//
//	names that have NOT yet been aliased
func (m *SqlSource) UnAliasedColumns() map[string]*Column {
	//u.Warnf("un-aliased %d", len(m.Source.Columns))
	if len(m.cols) > 0 || m.Source != nil && len(m.Source.Columns) == 0 {
		return m.cols
	}

	cols := make(map[string]*Column, len(m.Source.Columns))
	for _, col := range m.Source.Columns {
		_, right, hasLeft := col.LeftRight()
		//u.Debugf("aliasing: l:%q r:%q hasLeft?%v", left, right, hasLeft)
		if hasLeft {
			cols[right] = col
		} else {
			cols[right] = col
		}
	}
	return cols
}

// Get a list of Column names to position
func (m *SqlSource) ColumnPositions() map[string]int {
	if len(m.colIndex) > 0 {
		return m.colIndex
	}
	if m.Source == nil {
		return nil
	}
	cols := make(map[string]int)
	for idx, col := range m.Source.Columns {
		left, right, ok := col.LeftRight()
		//u.Debugf("aliasing: l:%v r:%v ok?%v", left, right, ok)
		if ok {
			cols[right] = idx
		} else {
			cols[left] = idx
		}
	}
	m.colIndex = cols
	return m.colIndex
}

// We need to be able to rewrite statements to convert a stmt such as:
//
//	FROM users AS u
//	    INNER JOIN orders AS o
//	    ON u.user_id = o.user_id
//
// So that we can evaluate the Join Key on left/right
// in this case, it is simple, just
//
//	=>   user_id
//
// or this one:
//
//			FROM users AS u
//				INNER JOIN orders AS o
//				ON LOWER(u.email) = LOWER(o.email)
//
//	   =>  LOWER(user_id)
func (m *SqlSource) JoinNodes() []expr.Node {
	return m.joinNodes
}
func (m *SqlSource) Finalize() error {
	if m.final {
		return nil
	}
	m.alias = strings.ToLower(m.Alias)
	if m.alias == "" {
		m.alias = strings.ToLower(m.Name)
	}
	//u.Warnf("finalize sqlsource: %v", len(m.Columns))
	m.final = true
	return nil
}
func (m *SqlSource) Equal(s *SqlSource) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}

	if m.final != s.final {
		return false
	}
	if m.alias != s.alias {
		return false
	}
	if m.Raw != s.Raw {
		return false
	}
	if m.Name != s.Name {
		return false
	}
	if m.Alias != s.Alias {
		return false
	}
	if m.Op != s.Op {
		return false
	}
	if m.LeftOrRight != s.LeftOrRight {
		return false
	}
	if m.JoinType != s.JoinType {
		return false
	}
	if m.Seekable != s.Seekable {
		return false
	}
	if m.JoinExpr != nil && !m.JoinExpr.Equal(s.JoinExpr) {
		return false
	}
	if len(m.cols) != len(s.cols) {
		return false
	}
	for k, c := range m.cols {
		sc, ok := s.cols[k]
		if !ok {
			return false
		}
		if !c.Equal(sc) {
			return false
		}
	}
	if len(m.colIndex) != len(s.colIndex) {
		return false
	}
	for k, midx := range m.colIndex {
		sidx, ok := s.colIndex[k]
		if !ok {
			return false
		}
		if midx != sidx {
			return false
		}
	}
	if len(m.joinNodes) != len(s.joinNodes) {
		return false
	}
	for i, jn := range m.joinNodes {
		if !jn.Equal(s.joinNodes[i]) {
			return false
		}
	}
	if !m.SubQuery.Equal(s.SubQuery) {
		return false
	}
	return true
}

func (m *SqlWhere) Keyword() lex.TokenType { return m.Op }
func (m *SqlWhere) writeDialectDepth(depth int, w expr.DialectWriter) {
	if int(m.Op) == 0 && m.Source == nil && m.Expr != nil {
		m.Expr.WriteDialect(w)
		return
	}
	// Op = subselect or in etc
	//  SELECT ... WHERE IN (SELECT ...)
	if int(m.Op) != 0 && m.Source != nil {
		io.WriteString(w, m.Op.String())
		io.WriteString(w, " (")
		m.Source.writeDialectDepth(depth+1, w)
		io.WriteString(w, ")")
		return
	}
	u.Errorf("unrecognized SqlWhere statement? %#v", m)
}
func (m *SqlWhere) WriteDialect(w expr.DialectWriter) { m.writeDialectDepth(0, w) }
func (m *SqlWhere) String() string {
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *SqlWhere) Equal(s *SqlWhere) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Op != s.Op {
		return false
	}
	if !m.Source.Equal(s.Source) {
		return false
	}
	if (m.Expr != nil && s.Expr == nil) || (m.Expr == nil && s.Expr != nil) {
		return false
	}
	if m.Expr != nil && !m.Expr.Equal(s.Expr) {
		return false
	}

	return true
}

func (m *SqlInto) Keyword() lex.TokenType            { return lex.TokenInto }
func (m *SqlInto) String() string                    { return fmt.Sprintf("%s", m.Table) }
func (m *SqlInto) WriteDialect(w expr.DialectWriter) {}
func (m *SqlInto) Equal(s *SqlInto) bool {
	if m == nil && s == nil {
		return true
	}
	if m == nil && s != nil {
		return false
	}
	if m != nil && s == nil {
		return false
	}
	if m.Table != s.Table {
		return false
	}
	return true
}

func (m *SqlInsert) Keyword() lex.TokenType { return m.kw }
func (m *SqlInsert) WriteDialect(w expr.DialectWriter) {

	io.WriteString(w, "INSERT INTO ")
	w.WriteIdentity(m.Table)
	io.WriteString(w, " (")

	for i, col := range m.Columns {
		if i > 0 {
			io.WriteString(w, ", ")
		}
		col.WriteDialect(w)
	}
	io.WriteString(w, ") VALUES")
	for i, row := range m.Rows {
		if i > 0 {
			io.WriteString(w, "\n\t,")
		}
		io.WriteString(w, " (")
		for vi, val := range row {
			if vi > 0 {
				io.WriteString(w, " ,")
			}
			if val.Expr != nil {
				val.Expr.WriteDialect(w)
			} else {
				// Value is not nil
				w.WriteValue(val.Value)
			}
		}
		w.Write([]byte{')'})
	}
}
func (m *SqlInsert) String() string {
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}

// RewriteAsPrepareable rewite the insert as a ? substituteable query
//
//	INSERT INTO user (name) VALUES ("wonder-woman") ->
//	   INSERT INTO user (name) VALUES (?)
func (m *SqlInsert) RewriteAsPrepareable(maxRows int, mark byte) string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("INSERT INTO %s (", m.Table))

	for i, col := range m.Columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(") VALUES")
	for i, row := range m.Rows {
		if maxRows > 0 && i >= maxRows {
			break
		}
		if i > 0 {
			buf.WriteString("\n\t,")
		}
		buf.WriteString(" (")
		for vi := range row {
			if vi > 0 {
				buf.WriteString(" ,")
			}
			buf.WriteByte(mark)
		}
		buf.WriteByte(')')
	}
	return buf.String()
}
func (m *SqlInsert) ColumnNames() []string {
	cols := make([]string, 0)
	for _, col := range m.Columns {
		cols = append(cols, col.Key())
	}
	return cols
}

func (m *SqlUpsert) Keyword() lex.TokenType            { return lex.TokenUpsert }
func (m *SqlUpsert) WriteDialect(w expr.DialectWriter) {}
func (m *SqlUpsert) String() string                    { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlUpsert) SqlSelect() *SqlSelect             { return sqlSelectFromWhere(m.Table, m.Where) }

func (m *SqlUpdate) Keyword() lex.TokenType { return lex.TokenUpdate }
func (m *SqlUpdate) WriteDialect(w expr.DialectWriter) {
	io.WriteString(w, "UPDATE ")
	w.WriteIdentity(m.Table)
	io.WriteString(w, " SET ")
	firstCol := true
	for key, val := range m.Values {
		if !firstCol {
			w.Write([]byte{',', ' '})
		}
		firstCol = false
		w.WriteIdentity(key)
		w.WriteValue(val.Value)
	}
	if m.Where != nil {
		io.WriteString(w, " WHERE ")
		m.Where.WriteDialect(w)
	}
}
func (m *SqlUpdate) String() string {
	w := expr.NewDefaultWriter()
	m.WriteDialect(w)
	return w.String()
}
func (m *SqlUpdate) SqlSelect() *SqlSelect { return sqlSelectFromWhere(m.Table, m.Where) }

func sqlSelectFromWhere(from string, where *SqlWhere) *SqlSelect {
	req := NewSqlSelect()
	req.From = []*SqlSource{NewSqlSource(from)}
	switch {
	case where.Expr != nil:
		req.Where = NewSqlWhere(where.Expr)
	default:
		req.Where = where
	}

	req.Star = true
	req.Columns = starCols
	return req
}

func (m *SqlDelete) Keyword() lex.TokenType            { return lex.TokenDelete }
func (m *SqlDelete) String() string                    { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDelete) WriteDialect(w expr.DialectWriter) {}

func (m *SqlDelete) SqlSelect() *SqlSelect { return sqlSelectFromWhere(m.Table, m.Where) }

func (m *SqlDescribe) Keyword() lex.TokenType            { return lex.TokenDescribe }
func (m *SqlDescribe) String() string                    { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlDescribe) WriteDialect(w expr.DialectWriter) {}

func (m *SqlShow) Keyword() lex.TokenType            { return lex.TokenShow }
func (m *SqlShow) String() string                    { return fmt.Sprintf("%s ", m.Keyword()) }
func (m *SqlShow) WriteDialect(w expr.DialectWriter) {}

func (m *CommandColumn) FingerPrint(r rune) string { return m.String() }
func (m *CommandColumn) String() string {
	if m.Expr != nil {
		return m.Expr.String()
	}
	if len(m.Name) > 0 {
		return m.Name
	}
	return ""
}
func (m *CommandColumn) WriteDialect(w expr.DialectWriter) {}
func (m *CommandColumn) Key() string {
	return m.Name
}

func (m *CommandColumns) WriteDialect(w expr.DialectWriter) {}
func (m *CommandColumns) String() string {
	colCt := len(*m)
	if colCt == 1 {
		return (*m)[0].String()
	} else if colCt == 0 {
		return ""
	}
	s := make([]string, len(*m))
	for i, col := range *m {
		s[i] = col.String()
	}
	return strings.Join(s, ", ")
}

func (m *SqlCommand) Keyword() lex.TokenType            { return m.kw }
func (m *SqlCommand) FingerPrint(r rune) string         { return m.String() }
func (m *SqlCommand) String() string                    { return fmt.Sprintf("%s %s", m.Keyword(), m.Columns.String()) }
func (m *SqlCommand) WriteDialect(w expr.DialectWriter) {}

func (m *SqlCreate) Keyword() lex.TokenType            { return lex.TokenCreate }
func (m *SqlCreate) FingerPrint(r rune) string         { return m.String() }
func (m *SqlCreate) String() string                    { return fmt.Sprintf("not-implemented") }
func (m *SqlCreate) WriteDialect(w expr.DialectWriter) {}

func (m *SqlDrop) Keyword() lex.TokenType            { return lex.TokenDrop }
func (m *SqlDrop) FingerPrint(r rune) string         { return m.String() }
func (m *SqlDrop) String() string                    { return fmt.Sprintf("DROP %s %v", m.Tok.T, m.Identity) }
func (m *SqlDrop) WriteDialect(w expr.DialectWriter) {}

func (m *SqlAlter) Keyword() lex.TokenType            { return lex.TokenAlter }
func (m *SqlAlter) FingerPrint(r rune) string         { return m.String() }
func (m *SqlAlter) String() string                    { return fmt.Sprintf("not-implemented") }
func (m *SqlAlter) WriteDialect(w expr.DialectWriter) {}

// Node serialization helpers
func tokenFromInt(iv int32) lex.Token {
	t, ok := lex.TokenNameMap[lex.TokenType(iv)]
	if ok {
		return lex.Token{T: t.T, V: t.Description}
	}
	return lex.Token{}
}

func optionalByte(b []byte) byte {
	var out byte
	if len(b) > 0 {
		return b[0]
	}
	return out
}

// EqualWith compare two with helpers for equality.
func EqualWith(l, r u.JsonHelper) bool {
	if len(l) != len(r) {
		return false
	}
	if len(l) == 0 && len(r) == 0 {
		return true
	}
	for k, lv := range l {
		rv, ok := r[k]
		if !ok {
			return false
		}
		switch lvt := lv.(type) {
		case int, int64, int32, string, bool, float64:
			if lv != rv {
				return false
			}
		case u.JsonHelper:
			rh, isHelper := rv.(u.JsonHelper)
			if !isHelper {
				return false
			}
			if !EqualWith(lvt, rh) {
				return false
			}
		case map[string]any:
			rh, isHelper := rv.(u.JsonHelper)
			if !isHelper {
				return false
			}
			if !EqualWith(u.JsonHelper(lvt), rh) {
				return false
			}
		default:
			u.Warnf("unhandled type comparison: %T", lv)
		}

	}
	return true
}

// HelperString Convert a Helper into key/value string
func HelperString(w expr.DialectWriter, jh u.JsonHelper) {

	// isJson := false
	// for k, v := range jh {
	// 	switch lvt := lv.(type) {
	// 	case int, int64, int32, string, bool, float64:
	// 		//
	// 	case []string, []int, []int32, []int64, []float64:
	// 		//
	// 	case u.JsonHelper, map[string]interface{}:
	// 		isJson = true
	// 		break
	// 	default:
	// 		u.Warnf("unhandled type comparison: %T", lv)
	// 	}
	// }
	pos := 0
	keys := jh.Keys()
	sort.Strings(keys)

	for _, k := range keys {
		val := jh[k]
		if pos > 0 {
			io.WriteString(w, ", ")
		}
		w.WriteIdentity(k)
		io.WriteString(w, " = ")
		switch v := val.(type) {
		case string:
			w.WriteLiteral(v)
		case int, int64, int32, bool, float64:
			io.WriteString(w, fmt.Sprintf("%v", v))
		default:
			u.Warnf("unhandled type comparison: %T", val)
		}
		pos++
	}
}
