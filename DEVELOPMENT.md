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

As of now, we are targeting 3 different stages of automation:
1. YugabyteDB inside TestContainers
2. CDCSDK Server inside TestContainers
3. Assertion of S3 values and their cleanup

Currently the 3rd part is automated as a part of `S3ConsumerRelIT.java` and to run the test follow these steps:
* Make sure you have the required creds setup in `~/.aws/credentials`
* Start a YugabyteDB instance on 127.0.0.1
* Create a table `test_table`
  `CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision);`
* Create CDC stream
* Unzip the cdcsdk-server and create configs for the same

The below command will create a docker image of CDCSDK Server and run
integration tests in cdcsdk-testing


   mvn integration-test -Drun.releaseTests


## Create a deploy a package

CDCSDK Server is distributed as an archive (tar.gz) and a Docker image. Use the
following commands to create and deploy both these artifacts.


    # Create cdcsdk-server-dist-<project.version>.tar.gz and
    # yugabyte/cdcsdk-server:<project.version>

    mvn package

    # Deploy docker image

    mvn deploy

    # Deploy archive to github project

    gh release create v<project.version> --generate-notes --title <An informative title about the major feature/bug> 'cdcsdk-server/cdcsdk-server-dist/target/cdcsdk-server-dist-<project.version>.tar.gz#CDCSDK Server'
