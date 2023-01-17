// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestIngestHandler(t *testing.T) {
	// prepare
	index, err := prepare.CreateIndex(time.Now().Format(consts.VersionTimeFmt))
	if err != nil {
		t.Fatalf("prepare index fail: %s", err.Error())
	}

	// test
	t.Run("test_ingest_handler", func(t *testing.T) {

		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "index", Value: index.Name})
		c.Params = p
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		c.Request.Body = io.NopCloser(bytes.NewBufferString(ingestRequest))
		IngestHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

const ingestRequest = `{
  "index": "search-engine",
  "documents": [
    {
      "name": "elasticsearch",
      "lang": "Java",
      "desc": "Free and Open, Distributed, RESTful Search Engine",
      "stars": 62411,
      "forks": 22629
    },
    {
      "name": "meilisearch",
      "lang": "Rust",
      "desc": "A lightning-fast search engine that fits effortlessly into your apps, websites, and workflow.",
      "stars": 31923,
      "forks": 1162
    },
    {
      "name": "cosmos",
      "lang": "C++",
      "desc": "World's largest Contributor driven code dataset | Used in Quark Search Engine, @OpenGenus IQ, OpenGenus Visual Project",
      "stars": 13300,
      "forks": 3620
    },
    {
      "name": "typesense",
      "lang": "C++",
      "desc": "Open Source alternative to Algolia and an Easier-to-Use alternative to ElasticSearch ⚡ 🔍 ✨ Fast, typo tolerant, in-memory fuzzy Search Engine for building delightful search experiences",
      "stars": 12089,
      "forks": 355
    },
    {
      "name": "chat-gpt-google-extension",
      "lang": "SCSS",
      "desc": "A browser extension to display ChatGPT response alongside search engine results",
      "stars": 8494,
      "forks": 554
    },
    {
      "name": "tantivy",
      "lang": "Rust",
      "desc": "Tantivy is a full-text search engine library inspired by Apache Lucene and written in Rust",
      "stars": 7485,
      "forks": 447
    },
    {
      "name": "OpenSearch",
      "lang": "Java",
      "desc": "🔎 Open source distributed and RESTful search engine.",
      "stars": 6262,
      "forks": 860
    },
    {
      "name": "riot",
      "lang": "Go",
      "desc": "Go Open Source, Distributed, Simple and efficient Search Engine; Warning: This is V1 and beta version, because of big memory consume, and the V2 will be rewrite all code.",
      "stars": 6078,
      "forks": 485
    },
    {
      "name": "lyra",
      "lang": "TypeScript",
      "desc": "🌌  Fast, in-memory, typo-tolerant, full-text search engine written in TypeScript.",
      "stars": 5156,
      "forks": 119
    },
    {
      "name": "awesome-hacker-search-engines",
      "lang": "",
      "desc": "A curated list of awesome search engines useful during Penetration testing, Vulnerability assessments, Red Team operations, Bug Bounty and more",
      "stars": 4100,
      "forks": 316
    },
    {
      "name": "RediSearch",
      "lang": "C",
      "desc": "A query and indexing engine for Redis, providing secondary indexing, full-text search, and aggregations. ",
      "stars": 4045,
      "forks": 433
    },
    {
      "name": "searxng",
      "lang": "Python",
      "desc": "SearXNG is a free internet metasearch engine which aggregates results from various search services and databases. Users are neither tracked nor profiled.",
      "stars": 3854,
      "forks": 602
    },
    {
      "name": "Toshi",
      "lang": "Rust",
      "desc": "A full-text search engine in rust",
      "stars": 3801,
      "forks": 121
    },
    {
      "name": "opengrok",
      "lang": "Java",
      "desc": "OpenGrok is a fast and usable source code search and cross reference engine, written in Java",
      "stars": 3779,
      "forks": 691
    },
    {
      "name": "qdrant",
      "lang": "Rust",
      "desc": "Qdrant - Vector Search Engine and Database for the next generation of AI applications. Also available in the cloud https://qdrant.to/cloud",
      "stars": 3667,
      "forks": 168
    },
    {
      "name": "weaviate",
      "lang": "Go",
      "desc": "Weaviate is an open source vector search engine that stores both objects and vectors, allowing for combining vector search with structured filtering with the fault-tolerance and scalability of a cloud-native database, all accessible through GraphQL, REST, and various language clients.",
      "stars": 3161,
      "forks": 181
    },
    {
      "name": "tntsearch",
      "lang": "PHP",
      "desc": "A fully featured full text search engine written in PHP",
      "stars": 2890,
      "forks": 265
    },
    {
      "name": "quickwit",
      "lang": "Rust",
      "desc": "Cloud-native search engine for log management \u0026 analytics",
      "stars": 2889,
      "forks": 149
    },
    {
      "name": "magnetico",
      "lang": "Go",
      "desc": "Autonomous (self-hosted) BitTorrent DHT search engine suite.",
      "stars": 2873,
      "forks": 351
    },
    {
      "name": "yacy_search_server",
      "lang": "Java",
      "desc": "Distributed Peer-to-Peer Web Search Engine and Intranet Search Appliance",
      "stars": 2867,
      "forks": 386
    },
    {
      "name": "Jets.js",
      "lang": "JavaScript",
      "desc": "Native CSS search engine",
      "stars": 2805,
      "forks": 119
    },
    {
      "name": "meta-tags",
      "lang": "Ruby",
      "desc": "Search Engine Optimization (SEO) for Ruby on Rails applications.",
      "stars": 2555,
      "forks": 285
    },
    {
      "name": "GoogleScraper",
      "lang": "HTML",
      "desc": "A Python module to scrape several search engines (like Google, Yandex, Bing, Duckduckgo, ...). Including asynchronous networking support.",
      "stars": 2449,
      "forks": 715
    },
    {
      "name": "tinysearch",
      "lang": "Rust",
      "desc": "🔍 Tiny, full-text search engine for static websites built with Rust and Wasm",
      "stars": 2208,
      "forks": 81
    },
    {
      "name": "minisearch",
      "lang": "JavaScript",
      "desc": "Tiny and powerful JavaScript full-text search engine for browser and Node",
      "stars": 2057,
      "forks": 72
    },
    {
      "name": "search-engine-optimization",
      "lang": "",
      "desc": ":mag: A helpful checklist/collection of Search Engine Optimization (SEO) tips and techniques.",
      "stars": 2053,
      "forks": 298
    },
    {
      "name": "ambar",
      "lang": "JavaScript",
      "desc": ":mag: Ambar: Document Search Engine",
      "stars": 1913,
      "forks": 366
    },
    {
      "name": "poseidon",
      "lang": "Go",
      "desc": "A search engine which can hold 100 trillion lines of log data.",
      "stars": 1880,
      "forks": 433
    },
    {
      "name": "retire",
      "lang": "Ruby",
      "desc": "A rich Ruby API and DSL for the Elasticsearch search engine",
      "stars": 1877,
      "forks": 541
    },
    {
      "name": "uBlock-Origin-dev-filter",
      "lang": "Python",
      "desc": "Filters to block and remove copycat-websites from DuckDuckGo, Google and other search engines. Specific to dev websites like StackOverflow or GitHub.",
      "stars": 1741,
      "forks": 42
    },
    {
      "name": "susper.com",
      "lang": "TypeScript",
      "desc": "Susper Decentralised Search Engine https://susper.com",
      "stars": 1735,
      "forks": 311
    },
    {
      "name": "query-server",
      "lang": "Python",
      "desc": "Query Server Search Engines https://query-server.herokuapp.com",
      "stars": 1654,
      "forks": 266
    },
    {
      "name": "metarank",
      "lang": "Scala",
      "desc": "A low code Machine Learning peersonalized ranking service for articles, listings, search results, recommendations that boosts user engagement. A friendly Learn-to-Rank engine",
      "stars": 1646,
      "forks": 61
    },
    {
      "name": "uncover",
      "lang": "Go",
      "desc": "Quickly discover exposed hosts on the internet using multiple search engines.",
      "stars": 1541,
      "forks": 129
    },
    {
      "name": "jekyll-seo-tag",
      "lang": "Ruby",
      "desc": "A Jekyll plugin to add metadata tags for search engines and social networks to better index and display your site's content.",
      "stars": 1475,
      "forks": 289
    },
    {
      "name": "open-source-search-engine",
      "lang": "C++",
      "desc": "Nov 20 2017 -- A distributed open source search engine and spider/crawler written in C/C++ for Linux on Intel/AMD. From gigablast dot com, which has binaries for download. See the README.md file at the very bottom of this page for instructions.",
      "stars": 1401,
      "forks": 444
    },
    {
      "name": "apollo",
      "lang": "Go",
      "desc": "A Unix-style personal search engine and web crawler for your digital footprint.",
      "stars": 1308,
      "forks": 48
    },
    {
      "name": "Dorks-collections-list",
      "lang": "",
      "desc": "List of Github repositories and articles with list of dorks for different search engines",
      "stars": 1245,
      "forks": 170
    },
    {
      "name": "monocle",
      "lang": "JavaScript",
      "desc": "Universal personal search engine, powered by a full text search algorithm written in pure Ink, indexing Linus's blogs and private note archives, contacts, tweets, and over a decade of journals.",
      "stars": 1244,
      "forks": 30
    },
    {
      "name": "diskover-community",
      "lang": "PHP",
      "desc": "Diskover Community Edition - Open source file indexer, file search engine and data management and analytics powered by Elasticsearch",
      "stars": 1239,
      "forks": 151
    },
    {
      "name": "orange",
      "lang": "Rust",
      "desc": "Cross-platform local file search engine.",
      "stars": 1232,
      "forks": 95
    },
    {
      "name": "rats-search",
      "lang": "JavaScript",
      "desc": "BitTorrent P2P multi-platform search engine for Desktop and Web servers with integrated torrent client.",
      "stars": 1207,
      "forks": 148
    },
    {
      "name": "ElasticPress",
      "lang": "PHP",
      "desc": "A fast and flexible search and query engine for WordPress.",
      "stars": 1161,
      "forks": 298
    },
    {
      "name": "spyglass",
      "lang": "Rust",
      "desc": "A personal search engine, crawl \u0026 index websites/files you want with a simple set of rules",
      "stars": 1156,
      "forks": 19
    },
    {
      "name": "vald",
      "lang": "Go",
      "desc": "Vald.  A Highly Scalable Distributed Vector Search Engine",
      "stars": 1026,
      "forks": 57
    },
    {
      "name": "torrent-net",
      "lang": "C",
      "desc": "Distributed search engines using BitTorrent and SQLite",
      "stars": 1006,
      "forks": 43
    },
    {
      "name": "opensse",
      "lang": "C++",
      "desc": "Open Sketch Search Engine－ 3D object retrieval based on sketch image as nput",
      "stars": 938,
      "forks": 167
    },
    {
      "name": "filterrific",
      "lang": "Ruby",
      "desc": "Filterrific is a Rails Engine plugin that makes it easy to filter, search, and sort your ActiveRecord lists.",
      "stars": 891,
      "forks": 117
    },
    {
      "name": "search-deflector",
      "lang": "D",
      "desc": "A small program that forwards searches from Cortana to your preferred browser and search engine.",
      "stars": 885,
      "forks": 53
    },
    {
      "name": "SCANNER-INURLBR",
      "lang": "PHP",
      "desc": "Advanced search in search engines, enables analysis provided to exploit GET / POST capturing emails \u0026 urls, with an internal custom validation junction for each target / url found.",
      "stars": 845,
      "forks": 408
    },
    {
      "name": "search_cop",
      "lang": "Ruby",
      "desc": "Search engine like fulltext query support for ActiveRecord",
      "stars": 776,
      "forks": 37
    },
    {
      "name": "ipfs-search",
      "lang": "Go",
      "desc": "Search engine for the Interplanetary Filesystem.",
      "stars": 758,
      "forks": 96
    },
    {
      "name": "tiefvision",
      "lang": "Lua",
      "desc": "End-to-end deep learning image-similarity search engine",
      "stars": 755,
      "forks": 191
    },
    {
      "name": "lnx",
      "lang": "Rust",
      "desc": "⚡ Insanely fast, 🌟 Feature-rich searching. lnx is the adaptable, typo tollerant deployment of the tantivy search engine.  Standing on the shoulders of giants.",
      "stars": 729,
      "forks": 31
    },
    {
      "name": "elasticsuite",
      "lang": "PHP",
      "desc": "Smile ElasticSuite - Magento 2 merchandising and search engine built on ElasticSearch",
      "stars": 726,
      "forks": 325
    },
    {
      "name": "groonga",
      "lang": "C",
      "desc": "An embeddable fulltext search engine. Groonga is the successor project to Senna.",
      "stars": 725,
      "forks": 116
    },
    {
      "name": "torrentinim",
      "lang": "HTML",
      "desc": "A very low memory-footprint, self hosted API-only torrent search engine. Sonarr + Radarr Compatible, native support for Linux, Mac and Windows.",
      "stars": 719,
      "forks": 34
    },
    {
      "name": "serpbear",
      "lang": "TypeScript",
      "desc": "Search Engine Position Rank Tracking App",
      "stars": 703,
      "forks": 38
    },
    {
      "name": "OnionSearch",
      "lang": "Python",
      "desc": "OnionSearch is a script that scrapes urls on different .onion search engines. ",
      "stars": 703,
      "forks": 108
    },
    {
      "name": "open-semantic-search",
      "lang": "Shell",
      "desc": "Open Source research tool to search, browse, analyze and explore large document collections by Semantic Search Engine and Open Source Text Mining \u0026 Text Analytics platform (Integrates ETL for document processing, OCR for images \u0026 PDF, named entity recognition for persons, organizations \u0026 locations, metadata management by thesaurus \u0026 ontologies, search user interface \u0026 search apps for fulltext search, faceted search \u0026 knowledge graph)",
      "stars": 687,
      "forks": 129
    },
    {
      "name": "CrossLinked",
      "lang": "Python",
      "desc": "LinkedIn enumeration tool to extract valid employee names from an organization through search engine scraping",
      "stars": 671,
      "forks": 132
    },
    {
      "name": "fullproof",
      "lang": "JavaScript",
      "desc": "javascript fulltext search engine library",
      "stars": 666,
      "forks": 48
    },
    {
      "name": "LucenePlusPlus",
      "lang": "C++",
      "desc": "Lucene++ is an up to date C++ port of the popular Java Lucene library, a high-performance, full-featured text search engine. ",
      "stars": 666,
      "forks": 221
    },
    {
      "name": "hoogle",
      "lang": "Haskell",
      "desc": "Haskell API search engine",
      "stars": 653,
      "forks": 120
    },
    {
      "name": "sis",
      "lang": "Python",
      "desc": "Simple image search engine",
      "stars": 649,
      "forks": 216
    },
    {
      "name": "cider",
      "lang": "Java",
      "desc": "\"Content Integration Framework: Document Extraction and Retrieval\" - A document parser framework that stores parsed entities into jena ( http://jena.sourceforge.net/ ) RDF vocabularies and provides knowledge-base enhanced semantic ananlysis of content. Annotated content can be used by search engines to present content navigation which will be implemented in the YaCy Search Engine",
      "stars": 646,
      "forks": 7
    },
    {
      "name": "macrobase",
      "lang": "Java",
      "desc": "MacroBase: A Search Engine for Fast Data",
      "stars": 643,
      "forks": 131
    },
    {
      "name": "mwmbl",
      "lang": "Python",
      "desc": "An open source, non-profit search engine implemented in python",
      "stars": 643,
      "forks": 20
    },
    {
      "name": "pastec",
      "lang": "C++",
      "desc": "Image recognition open source index and search engine",
      "stars": 610,
      "forks": 176
    },
    {
      "name": "ghostHunter",
      "lang": "JavaScript",
      "desc": "A Ghost blog search engine",
      "stars": 605,
      "forks": 125
    },
    {
      "name": "quark",
      "lang": "C++",
      "desc": "Stay happy while offline | World's first offline search engine. ",
      "stars": 601,
      "forks": 119
    },
    {
      "name": "awesome-seo",
      "lang": "JavaScript",
      "desc": ":star2: A curated list of SEO (Search Engine Optimization) links.",
      "stars": 561,
      "forks": 62
    },
    {
      "name": "resin",
      "lang": "C#",
      "desc": "Hardware-accelerated vector space search engine. Available as a HTTP service or as an embedded library.",
      "stars": 555,
      "forks": 41
    },
    {
      "name": "unigraph-dev",
      "lang": "TypeScript",
      "desc": "A local-first and universal knowledge graph, personal search engine, and workspace for your life.",
      "stars": 553,
      "forks": 34
    },
    {
      "name": "Ferret",
      "lang": "Go",
      "desc": "An optimized substring search engine written in Go",
      "stars": 545,
      "forks": 29
    },
    {
      "name": "liqe",
      "lang": "TypeScript",
      "desc": "Lightweight and performant Lucene-like parser, serializer and search engine.",
      "stars": 530,
      "forks": 11
    },
    {
      "name": "se-scraper",
      "lang": "HTML",
      "desc": "Javascript scraping module based on puppeteer for many different search engines...",
      "stars": 509,
      "forks": 121
    },
    {
      "name": "personfinder",
      "lang": "Python",
      "desc": "Person Finder is a searchable missing person database written in Python and hosted on App Engine.",
      "stars": 504,
      "forks": 209
    },
    {
      "name": "opensearchserver",
      "lang": "Java",
      "desc": "Open-source Enterprise Grade Search Engine Software",
      "stars": 476,
      "forks": 187
    },
    {
      "name": "wwsearch",
      "lang": "C++",
      "desc": "A full-text search engine supporting massive users, real-time updating, fast fuzzy matching and flexible table splitting.",
      "stars": 456,
      "forks": 86
    },
    {
      "name": "Goopt",
      "lang": "JavaScript",
      "desc": "🔍 Search Engine for a Procedural Simulation of the Web with GPT-3.",
      "stars": 456,
      "forks": 29
    },
    {
      "name": "picky",
      "lang": "HTML",
      "desc": "Picky is an easy to use and fast Ruby semantic search engine that helps your users find what they are looking for.",
      "stars": 445,
      "forks": 47
    },
    {
      "name": "milli",
      "lang": "Rust",
      "desc": "Search engine library for Meilisearch ⚡️",
      "stars": 441,
      "forks": 88
    },
    {
      "name": "visual_search",
      "lang": "JavaScript",
      "desc": "A visual search engine based on Elasticsearch and Tensorflow",
      "stars": 438,
      "forks": 128
    },
    {
      "name": "chatGPT-search-engine-extension",
      "lang": "JavaScript",
      "desc": "A browser extension to display ChatGPT response alongside Search Engine results",
      "stars": 430,
      "forks": 50
    },
    {
      "name": "obsidian-omnisearch",
      "lang": "TypeScript",
      "desc": "A search engine that \"just works\" for Obsidian. Includes OCR and PDF indexing.",
      "stars": 416,
      "forks": 22
    },
    {
      "name": "haystack",
      "lang": "",
      "desc": "Search engine for developers | Coming soon...",
      "stars": 415,
      "forks": 7
    },
    {
      "name": "learn-x-by-doing-y",
      "lang": "Python",
      "desc": "🛠️ Learn a technology X by doing a project  - Search engine of project-based learning",
      "stars": 413,
      "forks": 37
    },
    {
      "name": "PermissiveResearch",
      "lang": "Objective-C",
      "desc": "An iOS search engine that allows mistakes in the searched element. ",
      "stars": 411,
      "forks": 32
    },
    {
      "name": "searchlinkfix",
      "lang": "JavaScript",
      "desc": "DISCONTINUED: Browser extension that prevents Google and Yandex search pages from modifying search result links when you click them. This is useful when copying links but it also helps privacy by preventing the search engines from recording your clicks.",
      "stars": 406,
      "forks": 41
    },
    {
      "name": "amazon-autocomplete",
      "lang": "JavaScript",
      "desc": "🚀  Unlock the full power of the Amazon autocompletion engine right into your search input. JavaScript Plugin.",
      "stars": 393,
      "forks": 45
    },
    {
      "name": "Gochiusearch",
      "lang": "C#",
      "desc": "A Fast Scene Search Engine for Anime Series 'Gochuumon wa Usagi Desuka?'",
      "stars": 393,
      "forks": 23
    },
    {
      "name": "iffse",
      "lang": "Python",
      "desc": "Facial Feature Search Engine for Some Photo Sharing Website",
      "stars": 373,
      "forks": 60
    },
    {
      "name": "pouchdb-quick-search",
      "lang": "JavaScript",
      "desc": "Full-text search engine on top of PouchDB",
      "stars": 371,
      "forks": 86
    },
    {
      "name": "simplefts",
      "lang": "Go",
      "desc": "Simple Full-Text Search engine",
      "stars": 369,
      "forks": 37
    },
    {
      "name": "search-engine-parser",
      "lang": "Python",
      "desc": "Lightweight package to query popular search engines and scrape for result titles, links and descriptions",
      "stars": 368,
      "forks": 74
    },
    {
      "name": "Xapiand",
      "lang": "C++",
      "desc": "Xapiand: A RESTful Search Engine",
      "stars": 365,
      "forks": 32
    },
    {
      "name": "BitFunnel",
      "lang": "C++",
      "desc": "A signature-based search engine",
      "stars": 362,
      "forks": 37
    },
    {
      "name": "minsql",
      "lang": "Rust",
      "desc": "High-performance log search engine.",
      "stars": 359,
      "forks": 30
    },
    {
      "name": "librex",
      "lang": "PHP",
      "desc": "A privacy respecting free as in freedom meta search engine for Google, popular torrent sites and tor hidden services",
      "stars": 357,
      "forks": 45
    }
  ]
}`
