from nlp_services.discourse.entities import CoreferenceCountsService, EntityCountsService
from nlp_services.discourse.sentiment import DocumentSentimentService, DocumentEntitySentimentService, WpDocumentEntitySentimentService
from nlp_services.syntax import AllNounPhrasesService, AllVerbPhrasesService, HeadsService

config = {
             "region": "us-west-2",
             "price": "0.300",
             "ami": "ami-000f6d30",
             "key": "data-extraction",
             "sec": "sshable",
             "type": "m2.4xlarge",
             "tag": "data_extraction",
             "threshold": 50,
             "max_size": 5,
             "services": [
                 "AllNounPhrasesService",
                 "AllVerbPhrasesService",
                 "HeadsService",
                 "CoreferenceCountsService",
                 "EntityCountsService",
                 "DocumentSentimentService",
                 "DocumentEntitySentimentService",
                 "WpDocumentEntitySentimentService"
                         ]
         }
