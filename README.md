This project is reserved for scripts designed to be run against our NLP/data science pipeline.

Once completed, this project will fully deprecate the nlp-rest-client project along with the following repos:

* mrg_utils -- an s-expression parsing library capable of identifying semantic heads
(we can probably eventually deprecate this in favor of CoreNLP dependency parses)
* corenlp-xml-lib -- a data model for CoreNLP XML parses
* nlp_services -- a set of heavily-cached services that utilize the above data model
* WikiaAuthority -- a separate project that uses MediaWiki and Wikia APIs to identify
the most authoritative pages and authors for a given wiki
* wiki-recommender -- a set of recommendation prototypes that use data this library is responsible for generative

(Why yes, the nomenclature for the above projects should be standardized, thank you for noticing.)
