module github.com/lytics/qlbridge

go 1.23.0

toolchain go1.24.1

require (
	github.com/araddon/dateparse v0.0.0-20190622164848-0fb0a474d195
	github.com/araddon/gou v0.0.0-20211019181548-e7d08105776c
	github.com/blevesearch/bleve/v2 v2.4.4-0.20250311051258-dd102de3e3fd
	github.com/dchest/siphash v1.2.1
	github.com/go-sql-driver/mysql v1.4.1
	github.com/google/btree v1.0.0
	github.com/hashicorp/go-memdb v1.0.4
	github.com/jmespath/go-jmespath v0.4.0
	github.com/jmoiron/sqlx v1.2.0
	github.com/leekchan/timeutil v0.0.0-20150802142658-28917288c48d
	github.com/lytics/cloudstorage v0.2.16
	github.com/lytics/datemath v0.0.0-20180727225141-3ada1c10b5de
	github.com/mattn/go-sqlite3 v1.9.0
	github.com/mb0/glob v0.0.0-20160210091149-1eb79d2de6c4
	github.com/mssola/user_agent v0.5.0
	github.com/pborman/uuid v1.2.1
	github.com/stretchr/testify v1.10.0
	golang.org/x/net v0.37.0
	google.golang.org/api v0.226.0
	google.golang.org/protobuf v1.36.5
)

require (
	cloud.google.com/go v0.112.2 // indirect
	cloud.google.com/go/auth v0.15.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.7 // indirect
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	cloud.google.com/go/iam v1.1.6 // indirect
	cloud.google.com/go/storage v1.39.1 // indirect
	github.com/RoaringBitmap/roaring v1.9.3 // indirect
	github.com/RoaringBitmap/roaring/v2 v2.4.5 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/blevesearch/bleve_index_api v1.2.3 // indirect
	github.com/blevesearch/geo v0.1.20 // indirect
	github.com/blevesearch/go-faiss v1.0.24 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/gtreap v0.1.1 // indirect
	github.com/blevesearch/mmap-go v1.0.4 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.3.5 // indirect
	github.com/blevesearch/segment v0.9.1 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/upsidedown_store_api v1.0.2 // indirect
	github.com/blevesearch/vellum v1.1.0 // indirect
	github.com/blevesearch/zapx/v11 v11.4.1 // indirect
	github.com/blevesearch/zapx/v12 v12.4.1 // indirect
	github.com/blevesearch/zapx/v13 v13.4.1 // indirect
	github.com/blevesearch/zapx/v14 v14.4.1 // indirect
	github.com/blevesearch/zapx/v15 v15.4.1 // indirect
	github.com/blevesearch/zapx/v16 v16.2.2-0.20250305220028-89edb0ef9aa9 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/geo v0.0.0-20210211234256-740aa86cb551 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.5 // indirect
	github.com/googleapis/gax-go/v2 v2.14.1 // indirect
	github.com/hashicorp/go-immutable-radix v1.1.0 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.etcd.io/bbolt v1.3.7 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.59.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.59.0 // indirect
	go.opentelemetry.io/otel v1.34.0 // indirect
	go.opentelemetry.io/otel/metric v1.34.0 // indirect
	go.opentelemetry.io/otel/trace v1.34.0 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/oauth2 v0.28.0 // indirect
	golang.org/x/sync v0.12.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	golang.org/x/time v0.11.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20240213162025-012b6fc9bca9 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250106144421-5f5ef82da422 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/grpc v1.71.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/araddon/dateparse v0.0.0-20190622164848-0fb0a474d195 => github.com/lytics/dateparse v0.0.0-20241205004559-6cedc927c67b

replace github.com/araddon/gou v0.0.0-20190110011759-c797efecbb61 => github.com/lytics/gou v0.0.0-20220111003232-c7293e7b8946
