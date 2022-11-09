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
        node { label 'cdcsdk-docker-agent' }
    }
    environment {
        YB_VERSION_TO_TEST_AGAINST = "${params.YB_VERSION_TO_TEST_AGAINST}"
        DOCKER_IMAGE = "yugabyteci/yb_build_infra_almalinux8_x86_64:v2022-05-28T06_27_35"
        RELEASE_BUCKET_PATH = "s3://releases.yugabyte.com/cdcsdk-server"
        YUGABYTE_SRC = "/home/yugabyte"
    }
    stages {
        // stage("Cache Dependencies") {
        //     steps {
        //         cache (path: "$HOME/.m2/repository", key: "cdcsdk-${hashFiles('pom.xml')}") {
        //             sh 'mvn dependency:resolve'
        //         }
        //     }
        // }
        stage('Build and Test') {
            steps {
                script{
                    sh 'sudo chmod 666 /var/run/docker.sock'
                    sh 'sudo ./.github/scripts/install_start_yugabyte.sh ${YB_VERSION_TO_TEST_AGAINST} ${YUGABYTE_SRC}'
                    sh '''
                    PKG_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
                    ./.github/scripts/build_test_cdcsdk.sh ${PKG_VERSION}
                    '''
                }
            }
        }
        stage('Publish artifacts'){
            steps {
                script {
                    dir ("${env.WORKSPACE}/cdcsdk-server/cdcsdk-server-dist") {
                        if (params.PUBLISH_TO_S3) {
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
        success {
            slackSend(
                color: "good",
                channel: "#cdc-jenkins-runs",
                message: "CDC SDK daily master test Job Passed - ${BUILD_URL}."
            )
        }
        aborted {
            slackSend(
                color: "danger",
                channel: "#cdc-jenkins-runs",
                message: "CDC SDK daily master test Job Aborted - ${BUILD_URL}."
            )
        }
        failure {
            slackSend(
                color: "danger",
                channel: "#cdc-jenkins-runs",
                message: "CDC SDK daily master test Job Failed - ${BUILD_URL}."
            )
        }
    }
}