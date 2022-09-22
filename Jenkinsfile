// Copyright (c) YugaByte, Inc.

properties([
    parameters([
        string(defaultValue: 'main', description: 'Specify the Branch name', name: 'BRANCH'),
        string(defaultValue: '2.15.1.0-b175', description: 'Set the yugabyte version to test cdcsdk-server against e.g: 2.15.1.0-b175', name: 'YB_VERSION_TO_TEST_AGAINST'),
        booleanParam(defaultValue: false, description: 'If checked release builds are uploaded to s3 bucket. (cdcsdk-server -> s3://releases.yugabyte.com/cdcsdk-server)', name: 'PUBLISH_TO_S3')
    ])
])

pipeline {
     agent {
        node { label 'ybc-docker-agent' }
    }
    environment {
        YB_VERSION_TO_TEST_AGAINST = "${params.YB_VERSION_TO_TEST_AGAINST}"
        DOCKER_IMAGE = "yugabyteci/yb_build_infra_almalinux8_x86_64:v2022-05-28T06_27_35"
        RELEASE_BUCKET_PATH = "s3://releases.yugabyte.com/cdcsdk-server"
    }
    stages {
        stage('Build and Test') {
            steps {
                script{
                    env.PKG_VERSION = sh(script: "mvn help:evaluate -Dexpression=project.version -q -DforceStdout", returnStdout: true).trim()
                    env.JENKINS_AGENT_IP = sh(script: "hostname -i", returnStdout: true).trim()
                    sh './.github/scripts/linux_build.sh'
                }
            }
        }
        stage('Publish artifacts'){
            steps {
                script {
                    dir ("${env.WORKSPACE}/cdcsdk-server/cdcsdk-server-dist") {
                        if (env.PUBLISH_TO_S3) {
                            sh 'aws s3 cp --recursive --exclude="*" --include="*.gz" --include="*.gz.sha" --include="*.gz.md5" target ${RELEASE_BUCKET_PATH}/${PKG_VERSION}'
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: '**/*IT.txt,**/failsafe-summary.xml', fingerprint: true
            cleanWs()
        }
    }
}