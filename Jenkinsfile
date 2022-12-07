// Copyright (c) YugaByte, Inc.

properties([
    parameters([
        string(defaultValue: 'main', description: 'Specify the cdcsdk server branch name', name: 'SERVER_BRANCH'),
        string(defaultValue: 'main', description: 'Specify the cdcsdk testing branch name', name: 'TESTING_BRANCH'),
        string(defaultValue: '2.15.1.0-b175', description: 'Set the yugabyte version to test cdcsdk-server against e.g: 2.15.1.0-b175', name: 'YB_VERSION_TO_TEST_AGAINST'),
        booleanParam(defaultValue: false, description: 'If checked release builds are uploaded to s3 bucket. (cdcsdk-server -> s3://releases.yugabyte.com/cdcsdk-server)', name: 'PUBLISH_TO_S3')
    ])
])

pipeline {
     agent {
        node { label 'cdcsdk-docker-agent' }
    }
    options {
        timeout(time: 2, unit: 'HOURS')
    }
    environment {
        YB_VERSION_TO_TEST_AGAINST = "${params.YB_VERSION_TO_TEST_AGAINST}"
        RELEASE_BUCKET_PATH = "s3://releases.yugabyte.com/cdcsdk-server"
        YUGABYTE_SRC = "/home/centos/yugabyte"
        CDCSDK_SERVER_HOME = "$WORKSPACE/cdcsdk-server"
        CDCSDK_TESTING_HOME = "$WORKSPACE/cdcsdk-testing"
    }
    stages {
        stage('Clone Project') {
            steps {
                dir("${CDCSDK_SERVER_HOME}") {
                    git branch: '${SERVER_BRANCH}', url: 'https://github.com/yugabyte/cdcsdk-server.git'
                }
                dir("${CDCSDK_TESTING_HOME}") {
                    git branch: '${TESTING_BRANCH}', credentialsId: 'jenkins-user-key-vcs', url: 'git@github.com:yugabyte/cdcsdk-testing.git'
                }
            }
        }
        stage("Setup environment") {
            steps {
                script{
                    dir("${CDCSDK_SERVER_HOME}") {
                        sh './.github/scripts/install_prerequisites.sh'
                    }
                }
            }
        }
        stage("Cache Dependencies") {
            steps {
                dir("${CDCSDK_SERVER_HOME}") {
                    cache (path: "$HOME/.m2/repository", key: "cdcsdk-${hashFiles('pom.xml')}") {
                        sh 'mvn verify --fail-never -DskipTests -DskipITs'
                    }
                }
            }
        }
        stage('Build') {
            steps {
                script{
                    dir("${CDCSDK_SERVER_HOME}") {
                        env.PKG_VERSION = sh(script: "mvn help:evaluate -Dexpression=project.version -q -DforceStdout", returnStdout: true).trim()
                        sh './.github/scripts/install_start_yugabyte.sh ${YB_VERSION_TO_TEST_AGAINST} ${YUGABYTE_SRC}'
                        sh './.github/scripts/build_cdcsdk.sh ${PKG_VERSION}'
                    }
                }
            }
        }
        stage('Testing') {
            steps {
                script{
                     dir("${CDCSDK_TESTING_HOME}") {
                        env.CDCSDK_SERVER_IMAGE="quay.io/yugabyte/cdcsdk-server:latest"
                        sh 'mvn integration-test -Drun.releaseTests'
                    }
                }
            }
        }
        stage('Publish artifacts'){
            steps {
                script {
                    dir ("${CDCSDK_SERVER_HOME}/cdcsdk-server/cdcsdk-server-dist") {
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