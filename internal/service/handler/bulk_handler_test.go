// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestBulkHandler(t *testing.T) {
	count := 5
	versions := make([]string, count)
	for i := 0; i < count; i++ {
		versions[i] = time.Now().Format(time.RFC3339Nano)
		time.Sleep(time.Nanosecond * 1000)
	}
	indexes := make([]*core.Index, count)
	indexNames := make([]string, count)
	var err error
	for i := 0; i < count; i++ {
		indexes[i], err = prepare.CreateIndex(versions[i])
		if err != nil {
			t.Fatalf("prepare index and docs fail: %s", err.Error())
		}
		indexNames[i] = indexes[i].Name
	}

	// test
	t.Run("test_bulk_handler", func(t *testing.T) {

		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "index", Value: indexNames[0]})
		ingestReq := protocol.IngestRequest{}
		_ = json.Unmarshal(bytes.NewBufferString(ingestRequest).Bytes(), &ingestReq)
		var bytesBuffer bytes.Buffer
		for _, index := range indexNames {
			bulkAction := make(map[string]protocol.BulkMeta, 0)
			bulkAction["create"] = protocol.BulkMeta{Index: index}
			bulkActionJSON, _ := json.Marshal(bulkAction)

			for _, document := range ingestReq.Documents {
				documentJSON, _ := json.Marshal(document)
				bytesBuffer.Write(bulkActionJSON)
				bytesBuffer.WriteString("\n")
				bytesBuffer.Write(documentJSON)
				bytesBuffer.WriteString("\n")
			}
		}
		c.Params = p
		c.Request.Header.Set("Content-Type", "text/plain;charset=utf-8")
		c.Request.Body = io.NopCloser(bytes.NewReader(bytesBuffer.Bytes()))
		BulkHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

const bulkDocuments = `
{"desc":"Free and Open, Distributed, RESTful Search Engine","forks":22629,"lang":"Java","name":"elasticsearch","stars":62411}
{"desc":"A lightning-fast search engine that fits effortlessly into your apps, websites, and workflow.","forks":1162,"lang":"Rust","name":"meilisearch","stars":31923}
{"desc":"World's largest Contributor driven code dataset | Used in Quark Search Engine, @OpenGenus IQ, OpenGenus Visual Project","forks":3620,"lang":"C++","name":"cosmos","stars":13300}
{"desc":"Open Source alternative to Algolia and an Easier-to-Use alternative to ElasticSearch ⚡ 🔍 ✨ Fast, typo tolerant, in-memory fuzzy Search Engine for building delightful search experiences","forks":355,"lang":"C++","name":"typesense","stars":12089}
{"desc":"A browser extension to display ChatGPT response alongside search engine results","forks":554,"lang":"SCSS","name":"chat-gpt-google-extension","stars":8494}
{"desc":"Tantivy is a full-text search engine library inspired by Apache Lucene and written in Rust","forks":447,"lang":"Rust","name":"tantivy","stars":7485}
{"desc":"🔎 Open source distributed and RESTful search engine.","forks":860,"lang":"Java","name":"OpenSearch","stars":6262}
{"desc":"Go Open Source, Distributed, Simple and efficient Search Engine; Warning: This is V1 and beta version, because of big memory consume, and the V2 will be rewrite all code.","forks":485,"lang":"Go","name":"riot","stars":6078}
{"desc":"🌌  Fast, in-memory, typo-tolerant, full-text search engine written in TypeScript.","forks":119,"lang":"TypeScript","name":"lyra","stars":5156}
{"desc":"A curated list of awesome search engines useful during Penetration testing, Vulnerability assessments, Red Team operations, Bug Bounty and more","forks":316,"lang":"","name":"awesome-hacker-search-engines","stars":4100}
{"desc":"A query and indexing engine for Redis, providing secondary indexing, full-text search, and aggregations. ","forks":433,"lang":"C","name":"RediSearch","stars":4045}
{"desc":"SearXNG is a free internet metasearch engine which aggregates results from various search services and databases. Users are neither tracked nor profiled.","forks":602,"lang":"Python","name":"searxng","stars":3854}
{"desc":"A full-text search engine in rust","forks":121,"lang":"Rust","name":"Toshi","stars":3801}
{"desc":"OpenGrok is a fast and usable source code search and cross reference engine, written in Java","forks":691,"lang":"Java","name":"opengrok","stars":3779}
{"desc":"Qdrant - Vector Search Engine and Database for the next generation of AI applications. Also available in the cloud https://qdrant.to/cloud","forks":168,"lang":"Rust","name":"qdrant","stars":3667}
{"desc":"Weaviate is an open source vector search engine that stores both objects and vectors, allowing for combining vector search with structured filtering with the fault-tolerance and scalability of a cloud-native database, all accessible through GraphQL, REST, and various language clients.","forks":181,"lang":"Go","name":"weaviate","stars":3161}
{"desc":"A fully featured full text search engine written in PHP","forks":265,"lang":"PHP","name":"tntsearch","stars":2890}
{"desc":"Cloud-native search engine for log management \u0026 analytics","forks":149,"lang":"Rust","name":"quickwit","stars":2889}
{"desc":"Autonomous (self-hosted) BitTorrent DHT search engine suite.","forks":351,"lang":"Go","name":"magnetico","stars":2873}
{"desc":"Distributed Peer-to-Peer Web Search Engine and Intranet Search Appliance","forks":386,"lang":"Java","name":"yacy_search_server","stars":2867}
{"desc":"Native CSS search engine","forks":119,"lang":"JavaScript","name":"Jets.js","stars":2805}
{"desc":"Search Engine Optimization (SEO) for Ruby on Rails applications.","forks":285,"lang":"Ruby","name":"meta-tags","stars":2555}
{"desc":"A Python module to scrape several search engines (like Google, Yandex, Bing, Duckduckgo, ...). Including asynchronous networking support.","forks":715,"lang":"HTML","name":"GoogleScraper","stars":2449}
{"desc":"🔍 Tiny, full-text search engine for static websites built with Rust and Wasm","forks":81,"lang":"Rust","name":"tinysearch","stars":2208}
{"desc":"Tiny and powerful JavaScript full-text search engine for browser and Node","forks":72,"lang":"JavaScript","name":"minisearch","stars":2057}
{"desc":":mag: A helpful checklist/collection of Search Engine Optimization (SEO) tips and techniques.","forks":298,"lang":"","name":"search-engine-optimization","stars":2053}
{"desc":":mag: Ambar: Document Search Engine","forks":366,"lang":"JavaScript","name":"ambar","stars":1913}
{"desc":"A search engine which can hold 100 trillion lines of log data.","forks":433,"lang":"Go","name":"poseidon","stars":1880}
{"desc":"A rich Ruby API and DSL for the Elasticsearch search engine","forks":541,"lang":"Ruby","name":"retire","stars":1877}
{"desc":"Filters to block and remove copycat-websites from DuckDuckGo, Google and other search engines. Specific to dev websites like StackOverflow or GitHub.","forks":42,"lang":"Python","name":"uBlock-Origin-dev-filter","stars":1741}
{"desc":"Susper Decentralised Search Engine https://susper.com","forks":311,"lang":"TypeScript","name":"susper.com","stars":1735}
{"desc":"Query Server Search Engines https://query-server.herokuapp.com","forks":266,"lang":"Python","name":"query-server","stars":1654}
{"desc":"A low code Machine Learning peersonalized ranking service for articles, listings, search results, recommendations that boosts user engagement. A friendly Learn-to-Rank engine","forks":61,"lang":"Scala","name":"metarank","stars":1646}
{"desc":"Quickly discover exposed hosts on the internet using multiple search engines.","forks":129,"lang":"Go","name":"uncover","stars":1541}
{"desc":"A Jekyll plugin to add metadata tags for search engines and social networks to better index and display your site's content.","forks":289,"lang":"Ruby","name":"jekyll-seo-tag","stars":1475}
{"desc":"Nov 20 2017 -- A distributed open source search engine and spider/crawler written in C/C++ for Linux on Intel/AMD. From gigablast dot com, which has binaries for download. See the README.md file at the very bottom of this page for instructions.","forks":444,"lang":"C++","name":"open-source-search-engine","stars":1401}
{"desc":"A Unix-style personal search engine and web crawler for your digital footprint.","forks":48,"lang":"Go","name":"apollo","stars":1308}
{"desc":"List of Github repositories and articles with list of dorks for different search engines","forks":170,"lang":"","name":"Dorks-collections-list","stars":1245}
{"desc":"Universal personal search engine, powered by a full text search algorithm written in pure Ink, indexing Linus's blogs and private note archives, contacts, tweets, and over a decade of journals.","forks":30,"lang":"JavaScript","name":"monocle","stars":1244}
{"desc":"Diskover Community Edition - Open source file indexer, file search engine and data management and analytics powered by Elasticsearch","forks":151,"lang":"PHP","name":"diskover-community","stars":1239}
{"desc":"Cross-platform local file search engine.","forks":95,"lang":"Rust","name":"orange","stars":1232}
{"desc":"BitTorrent P2P multi-platform search engine for Desktop and Web servers with integrated torrent client.","forks":148,"lang":"JavaScript","name":"rats-search","stars":1207}
{"desc":"A fast and flexible search and query engine for WordPress.","forks":298,"lang":"PHP","name":"ElasticPress","stars":1161}
{"desc":"A personal search engine, crawl \u0026 index websites/files you want with a simple set of rules","forks":19,"lang":"Rust","name":"spyglass","stars":1156}
{"desc":"Vald.  A Highly Scalable Distributed Vector Search Engine","forks":57,"lang":"Go","name":"vald","stars":1026}
{"desc":"Distributed search engines using BitTorrent and SQLite","forks":43,"lang":"C","name":"torrent-net","stars":1006}
{"desc":"Open Sketch Search Engine－ 3D object retrieval based on sketch image as nput","forks":167,"lang":"C++","name":"opensse","stars":938}
{"desc":"Filterrific is a Rails Engine plugin that makes it easy to filter, search, and sort your ActiveRecord lists.","forks":117,"lang":"Ruby","name":"filterrific","stars":891}
{"desc":"A small program that forwards searches from Cortana to your preferred browser and search engine.","forks":53,"lang":"D","name":"search-deflector","stars":885}
{"desc":"Advanced search in search engines, enables analysis provided to exploit GET / POST capturing emails \u0026 urls, with an internal custom validation junction for each target / url found.","forks":408,"lang":"PHP","name":"SCANNER-INURLBR","stars":845}
{"desc":"Search engine like fulltext query support for ActiveRecord","forks":37,"lang":"Ruby","name":"search_cop","stars":776}
{"desc":"Search engine for the Interplanetary Filesystem.","forks":96,"lang":"Go","name":"ipfs-search","stars":758}
{"desc":"End-to-end deep learning image-similarity search engine","forks":191,"lang":"Lua","name":"tiefvision","stars":755}
{"desc":"⚡ Insanely fast, 🌟 Feature-rich searching. lnx is the adaptable, typo tollerant deployment of the tantivy search engine.  Standing on the shoulders of giants.","forks":31,"lang":"Rust","name":"lnx","stars":729}
{"desc":"Smile ElasticSuite - Magento 2 merchandising and search engine built on ElasticSearch","forks":325,"lang":"PHP","name":"elasticsuite","stars":726}
{"desc":"An embeddable fulltext search engine. Groonga is the successor project to Senna.","forks":116,"lang":"C","name":"groonga","stars":725}
{"desc":"A very low memory-footprint, self hosted API-only torrent search engine. Sonarr + Radarr Compatible, native support for Linux, Mac and Windows.","forks":34,"lang":"HTML","name":"torrentinim","stars":719}
{"desc":"Search Engine Position Rank Tracking App","forks":38,"lang":"TypeScript","name":"serpbear","stars":703}
{"desc":"OnionSearch is a script that scrapes urls on different .onion search engines. ","forks":108,"lang":"Python","name":"OnionSearch","stars":703}
{"desc":"Open Source research tool to search, browse, analyze and explore large document collections by Semantic Search Engine and Open Source Text Mining \u0026 Text Analytics platform (Integrates ETL for document processing, OCR for images \u0026 PDF, named entity recognition for persons, organizations \u0026 locations, metadata management by thesaurus \u0026 ontologies, search user interface \u0026 search apps for fulltext search, faceted search \u0026 knowledge graph)","forks":129,"lang":"Shell","name":"open-semantic-search","stars":687}
{"desc":"LinkedIn enumeration tool to extract valid employee names from an organization through search engine scraping","forks":132,"lang":"Python","name":"CrossLinked","stars":671}
{"desc":"javascript fulltext search engine library","forks":48,"lang":"JavaScript","name":"fullproof","stars":666}
{"desc":"Lucene++ is an up to date C++ port of the popular Java Lucene library, a high-performance, full-featured text search engine. ","forks":221,"lang":"C++","name":"LucenePlusPlus","stars":666}
{"desc":"Haskell API search engine","forks":120,"lang":"Haskell","name":"hoogle","stars":653}
{"desc":"Simple image search engine","forks":216,"lang":"Python","name":"sis","stars":649}
{"desc":"\"Content Integration Framework: Document Extraction and Retrieval\" - A document parser framework that stores parsed entities into jena ( http://jena.sourceforge.net/ ) RDF vocabularies and provides knowledge-base enhanced semantic ananlysis of content. Annotated content can be used by search engines to present content navigation which will be implemented in the YaCy Search Engine","forks":7,"lang":"Java","name":"cider","stars":646}
{"desc":"MacroBase: A Search Engine for Fast Data","forks":131,"lang":"Java","name":"macrobase","stars":643}
{"desc":"An open source, non-profit search engine implemented in python","forks":20,"lang":"Python","name":"mwmbl","stars":643}
{"desc":"Image recognition open source index and search engine","forks":176,"lang":"C++","name":"pastec","stars":610}
{"desc":"A Ghost blog search engine","forks":125,"lang":"JavaScript","name":"ghostHunter","stars":605}
{"desc":"Stay happy while offline | World's first offline search engine. ","forks":119,"lang":"C++","name":"quark","stars":601}
{"desc":":star2: A curated list of SEO (Search Engine Optimization) links.","forks":62,"lang":"JavaScript","name":"awesome-seo","stars":561}
{"desc":"Hardware-accelerated vector space search engine. Available as a HTTP service or as an embedded library.","forks":41,"lang":"C#","name":"resin","stars":555}
{"desc":"A local-first and universal knowledge graph, personal search engine, and workspace for your life.","forks":34,"lang":"TypeScript","name":"unigraph-dev","stars":553}
{"desc":"An optimized substring search engine written in Go","forks":29,"lang":"Go","name":"Ferret","stars":545}
{"desc":"Lightweight and performant Lucene-like parser, serializer and search engine.","forks":11,"lang":"TypeScript","name":"liqe","stars":530}
{"desc":"Javascript scraping module based on puppeteer for many different search engines...","forks":121,"lang":"HTML","name":"se-scraper","stars":509}
{"desc":"Person Finder is a searchable missing person database written in Python and hosted on App Engine.","forks":209,"lang":"Python","name":"personfinder","stars":504}
{"desc":"Open-source Enterprise Grade Search Engine Software","forks":187,"lang":"Java","name":"opensearchserver","stars":476}
{"desc":"A full-text search engine supporting massive users, real-time updating, fast fuzzy matching and flexible table splitting.","forks":86,"lang":"C++","name":"wwsearch","stars":456}
{"desc":"🔍 Search Engine for a Procedural Simulation of the Web with GPT-3.","forks":29,"lang":"JavaScript","name":"Goopt","stars":456}
{"desc":"Picky is an easy to use and fast Ruby semantic search engine that helps your users find what they are looking for.","forks":47,"lang":"HTML","name":"picky","stars":445}
{"desc":"Search engine library for Meilisearch ⚡️","forks":88,"lang":"Rust","name":"milli","stars":441}
{"desc":"A visual search engine based on Elasticsearch and Tensorflow","forks":128,"lang":"JavaScript","name":"visual_search","stars":438}
{"desc":"A browser extension to display ChatGPT response alongside Search Engine results","forks":50,"lang":"JavaScript","name":"chatGPT-search-engine-extension","stars":430}
{"desc":"A search engine that \"just works\" for Obsidian. Includes OCR and PDF indexing.","forks":22,"lang":"TypeScript","name":"obsidian-omnisearch","stars":416}
{"desc":"Search engine for developers | Coming soon...","forks":7,"lang":"","name":"haystack","stars":415}
{"desc":"🛠️ Learn a technology X by doing a project  - Search engine of project-based learning","forks":37,"lang":"Python","name":"learn-x-by-doing-y","stars":413}
{"desc":"An iOS search engine that allows mistakes in the searched element. ","forks":32,"lang":"Objective-C","name":"PermissiveResearch","stars":411}
{"desc":"DISCONTINUED: Browser extension that prevents Google and Yandex search pages from modifying search result links when you click them. This is useful when copying links but it also helps privacy by preventing the search engines from recording your clicks.","forks":41,"lang":"JavaScript","name":"searchlinkfix","stars":406}
{"desc":"🚀  Unlock the full power of the Amazon autocompletion engine right into your search input. JavaScript Plugin.","forks":45,"lang":"JavaScript","name":"amazon-autocomplete","stars":393}
{"desc":"A Fast Scene Search Engine for Anime Series 'Gochuumon wa Usagi Desuka?'","forks":23,"lang":"C#","name":"Gochiusearch","stars":393}
{"desc":"Facial Feature Search Engine for Some Photo Sharing Website","forks":60,"lang":"Python","name":"iffse","stars":373}
{"desc":"Full-text search engine on top of PouchDB","forks":86,"lang":"JavaScript","name":"pouchdb-quick-search","stars":371}
{"desc":"Simple Full-Text Search engine","forks":37,"lang":"Go","name":"simplefts","stars":369}
{"desc":"Lightweight package to query popular search engines and scrape for result titles, links and descriptions","forks":74,"lang":"Python","name":"search-engine-parser","stars":368}
{"desc":"Xapiand: A RESTful Search Engine","forks":32,"lang":"C++","name":"Xapiand","stars":365}
{"desc":"A signature-based search engine","forks":37,"lang":"C++","name":"BitFunnel","stars":362}
{"desc":"High-performance log search engine.","forks":30,"lang":"Rust","name":"minsql","stars":359}
{"desc":"A privacy respecting free as in freedom meta search engine for Google, popular torrent sites and tor hidden services","forks":45,"lang":"PHP","name":"librex","stars":357}

`
