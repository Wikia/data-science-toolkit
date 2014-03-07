config = {
    "region": "us-west-2",
    "price": "0.300",
    "ami": "ami-1c7c132c",  # base-wdx-140305b
    "key": "data-extraction",
    "sec": "sshable",
    "type": "m2.4xlarge",
    "tag": "wiki_data_extraction",
    "threshold": 50,
    "max_size": 5,
    "services": [
        "TopEntitiesService",
        "EntityDocumentCountsService",
        "TopHeadsService",
        "WpTopEntitiesService",
        "WpEntityDocumentCountsService",
        "WikiEntitySentimentService",
        "WpWikiEntitySentimentService",
        "AllEntitiesSentimentAndCountsService"
        ]
    }
