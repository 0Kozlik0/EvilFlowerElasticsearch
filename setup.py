from setuptools import setup, find_packages

setup(
    name="elvira-elasticsearch-client",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "elasticsearch[async]>=8.6.0",
        "python-decouple>=3.6"
    ],
    python_requires=">=3.8",
    author="Patrik Kozlík",
    author_email="xkozlik@stuba.sk",
    description="A shared Elasticsearch client for Elvira services",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/0Kozlik0/EvilFlowerElasticsearch",
    package_data={"elvira_elasticsearch_client": ["py.typed"]},
    zip_safe=False,
)