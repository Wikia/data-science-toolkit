config = {
    "region": "us-west-2",
    "price": "0.300",
    "ami": "ami-611e6851",  # dstk tristan
    "key": "data-extraction",
    "sec": "sshable",
    "type": "m2.4xlarge",
    "tag": "wiki_data_extraction",
    "threshold": 50,
    "git_ref": "page-rec",  # master
    "max_size": 1,  # 5
    "services": ",".join([
        "TopEntitiesService",
        "EntityDocumentCountsService",
        "TopHeadsService",
        "WpTopEntitiesService",
        "WpEntityDocumentCountsService",
        "WikiEntitySentimentService",
        "WpWikiEntitySentimentService",
        "AllEntitiesSentimentAndCountsService"
    ])
}
