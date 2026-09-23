from setuptools import setup, find_packages

setup(
    name="weenspace-queue",
    version="0.1.3",
    packages=find_packages(),
    install_requires=[
        "python-qpid-proton==0.40.0",
        "rabbitmq-amqp-python-client==1.0.1",
    ],
    extras_require={
        "aws": ["boto3==1.43.100"],
    }
)