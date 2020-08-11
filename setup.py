from setuptools import setup

setup(
    name='hyperleaup',
    version='0.0.1',
    packages=['hyperleaup'],
    url='https://github.com/goodwillpunning/hyperleaup',
    license='None',
    author='willgirten',
    author_email='will.girten@databricks.com',
    description='Create and publish Tableau Hyper files from Apache Spark DataFrames and Spark SQL.',
    install_requires=['pyspark', 'tableauhyperapi', 'requests', 'tableauserverclient'],
    zip_safe=False
)
