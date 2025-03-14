module github.com/lytics/qlbridge

go 1.21

toolchain go1.22.5

require (
	github.com/araddon/dateparse v0.0.0-20190622164848-0fb0a474d195
	github.com/araddon/gou v0.0.0-20190110011759-c797efecbb61
	github.com/blevesearch/bleve/v2 v2.4.4
	github.com/dchest/siphash v1.2.1
	github.com/go-sql-driver/mysql v1.4.1
	github.com/google/btree v1.0.0
	github.com/hashicorp/go-memdb v1.0.4
	github.com/jmespath/go-jmespath v0.0.0-20180206201540-c2b33e8439af
	github.com/jmoiron/sqlx v1.2.0
	github.com/leekchan/timeutil v0.0.0-20150802142658-28917288c48d
	github.com/lytics/cloudstorage v0.2.1
	github.com/lytics/datemath v0.0.0-20180727225141-3ada1c10b5de
	github.com/mattn/go-sqlite3 v1.9.0
	github.com/mb0/glob v0.0.0-20160210091149-1eb79d2de6c4
	github.com/mssola/user_agent v0.5.0
	github.com/pborman/uuid v1.2.0
	github.com/stretchr/testify v1.8.1
	golang.org/x/net v0.0.0-20191021144547-ec77196f6094
	google.golang.org/api v0.11.0
	google.golang.org/protobuf v1.35.1
)

require (
	cloud.google.com/go v0.38.0 // indirect
	github.com/RoaringBitmap/roaring v1.9.3 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/blevesearch/bleve_index_api v1.1.12 // indirect
	github.com/blevesearch/geo v0.1.20 // indirect
	github.com/blevesearch/go-faiss v1.0.24 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/gtreap v0.1.1 // indirect
	github.com/blevesearch/mmap-go v1.0.4 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.2.16 // indirect
	github.com/blevesearch/segment v0.9.1 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/upsidedown_store_api v1.0.2 // indirect
	github.com/blevesearch/vellum v1.0.10 // indirect
	github.com/blevesearch/zapx/v11 v11.3.10 // indirect
	github.com/blevesearch/zapx/v12 v12.3.10 // indirect
	github.com/blevesearch/zapx/v13 v13.3.10 // indirect
	github.com/blevesearch/zapx/v14 v14.3.10 // indirect
	github.com/blevesearch/zapx/v15 v15.3.16 // indirect
	github.com/blevesearch/zapx/v16 v16.1.9-0.20241217210638-a0519e7caf3b // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/geo v0.0.0-20210211234256-740aa86cb551 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/uuid v1.0.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/hashicorp/go-immutable-radix v1.1.0 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.etcd.io/bbolt v1.3.7 // indirect
	go.mongodb.org/mongo-driver v1.7.2 // indirect
	go.opencensus.io v0.21.0 // indirect
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	google.golang.org/appengine v1.5.0 // indirect
	google.golang.org/genproto v0.0.0-20190502173448-54afdca5d873 // indirect
	google.golang.org/grpc v1.20.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/araddon/dateparse v0.0.0-20190622164848-0fb0a474d195 => github.com/lytics/dateparse v0.0.0-20241205004559-6cedc927c67b

replace github.com/araddon/gou v0.0.0-20190110011759-c797efecbb61 => github.com/lytics/gou v0.0.0-20220111003232-c7293e7b8946
