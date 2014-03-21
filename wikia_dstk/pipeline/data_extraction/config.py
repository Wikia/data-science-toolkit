default_config = {
    "queue": "data_events",
    "region": "us-west-2",
    "price": "0.300",
    "ami": "ami-d6e785e6",
    "key": "data-extraction",
    "sec": "sshable",
    "type": "m2.4xlarge",
    "tag": "data_extraction",
    "threshold": 50,
    "max-size": 5,
    "services": ",".join([
        "AllNounPhrasesService",
        "AllVerbPhrasesService",
        "HeadsService",
        "CoreferenceCountsService",
        "EntityCountsService",
        "DocumentSentimentService",
        "DocumentEntitySentimentService",
        "WpDocumentEntitySentimentService"
    ])
}