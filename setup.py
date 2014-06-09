from setuptools import setup

setup(name="wikia_dstk",
      version="0.0.2",
      author="Robert Elwell",
      author_email="robert.elwell@gmail.com",
      description="A library for automated data science scripts that rely on NLP libs, etc.",
      license="Other",
      url="https://github.com/Wikia/data-science-toolkit",
      packages=[
          "wikia_dstk", "wikia_dstk.lda", "wikia_dstk.pipeline",
          "wikia_dstk.pipeline.event", "wikia_dstk.pipeline.parser",
          "wikia_dstk.pipeline.data_extraction",
          "wikia_dstk.pipeline.wiki_data_extraction", "wikia_dstk.authority",
          "wikia_dstk.recommendations", "wikia_dstk.loadbalancing",
          "wikia_dstk.knowledge_graph"],
      install_requires=[
          "nlp_services>=0.0.1", "pyro4", "gensim", "scikit-learn", "requests",
          "boto", "nltk"],
      dependency_links=[
          "https://github.com/relwell/nlp_services/archive/master.zip#egg=nlp_services-0.0.1"
      ]
      )
