# Developer Workflow

## Setup S3 Repository access to download Yugabyte Connector


    mkdir -p ~/.aws
    touch ~/.aws/credentials


Copy credentials from [AWS Management
Console|https://aws.amazon.com/blogs/security/aws-single-sign-on-now-enables-command-line-interface-access-for-aws-accounts-using-corporate-credentials/]


Follow instructions to create a profile in `~/.aws/credentials`. Change the
profile to default. The file should look like:


    [default]
    aws_access_key_id=...
    aws_secret_access_key=...
    aws_session_token=...


**Note that these credentials and temporary and you may have to regularly pick
up new credentials**

## Integration Tests


    mvn integration-test
    # Run a specific integration test
    mvn integration-test -Dit.test=HttpIT -DfailIfNoTests=false


## Run Release Tests

The below command will create a docker image of CDCSDK Server and run
integration tests in cdcsdk-testing


   mvn integration-test -Drun.releaseTests
