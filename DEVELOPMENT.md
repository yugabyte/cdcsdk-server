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

