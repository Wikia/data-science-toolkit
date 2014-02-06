from setuptools import setup

setup(
    name="wikia_dstk",
    version= "0.0.1",
    author="Robert Elwell",
    author_email="robert.elwell@gmail.com",
    description="A library for automated data science scripts that rely on NLP libs, etc.",
    license="Other",
    packages=["wikia_dstk.lda"],
    install_requires=["nlp_services", "pyro4", "gensim", "scikit-learn", "requests"],
    dependency_links=["https://github.com/relwell/nlp_services/archive/master.zip#egg=nlp_services-0.0.1"],
    )