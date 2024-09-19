import setuptools

setuptools.setup(
    name="django_querysets_single_query_fetch",
    version="0.0.12",
    description="Execute multiple Django querysets in a single SQL query",
    long_description="Utility which executes multiple Django querysets over a single network/query call and returns results which would have been returned in normal evaluation of querysets",
    author="Nishant Singh",
    author_email="nishant.singh@mydukaan.io",
    license="Apache Software License",
    packages=["django_querysets_single_query_fetch"],
    zip_safe=False,
    install_requires=["django>=4.0,<5.2"],
    python_requires=">=3.9",
    include_package_data=True,
    package_data={},
)
