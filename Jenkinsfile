// Copyright (c) YugaByte, Inc.

properties([
    parameters([
        string(defaultValue: 'main', description: 'Specify the cdcsdk server branch name', name: 'SERVER_BRANCH'),
        string(defaultValue: 'main', description: 'Specify the cdcsdk testing branch name', name: 'TESTING_BRANCH'),
        string(defaultValue: 'main', description: 'Specify the debezium connector branch name', name: 'DEBEZIUM_CONNECTOR_BRANCH'),
        string(defaultValue: '2.17.1.0', description: 'Set the yugabyte version to test cdcsdk-server against e.g: 2.17.1.0', name: 'YB_VERSION_TO_TEST_AGAINST'),
        booleanParam(defaultValue: false, description: 'If checked release builds are uploaded to s3 bucket. (cdcsdk-server -> s3://releases.yugabyte.com/cdcsdk-server)', name: 'PUBLISH_TO_S3')
    ])
])

pipeline {
     agent {
        node { label 'cdcsdk-docker-agent' }
    }
    options {
        timeout(time: 4, unit: 'HOURS')
        parallelsAlwaysFailFast()
    }
    environment {
        GCLOUD_PROJECT = "yugabyte"
        YB_VERSION_TO_TEST_AGAINST = "${params.YB_VERSION_TO_TEST_AGAINST}"
        RELEASE_BUCKET_PATH = "s3://releases.yugabyte.com/cdcsdk-server"
        YUGABYTE_SRC = "/home/ec2-user/yugabyte"
        CDCSDK_SERVER_HOME = "$WORKSPACE/cdcsdk-server"
        CDCSDK_TESTING_HOME = "$WORKSPACE/cdcsdk-testing"
        DEBEZIUM_CONNECTOR_HOME = "$WORKSPACE/debezium-connector-yugabytedb"
        SUBSCRIPTION_ID = "dbserver1.public.test_table-sub"
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
                dir("${DEBEZIUM_CONNECTOR_HOME}") {
                    git branch: '${DEBEZIUM_CONNECTOR_BRANCH}', url: 'https://github.com/yugabyte/debezium-connector-yugabytedb.git'
                }
            }
        }
        stage("Setup environment") {
            steps {
                withCredentials([file(credentialsId: 'debezium-quay-auth', variable: 'debezium_quay_auth')]) {
                    sh 'mkdir -p $HOME/.docker'
                    sh 'cp ${debezium_quay_auth} $HOME/.docker/config.json'
                    sh 'chmod 600 $HOME/.docker/config.json'
                }
                script{
                    //dir("${CDCSDK_SERVER_HOME}") {
                        sh './.github/scripts/install_prerequisites.sh'
                        sh './.github/scripts/install_start_yugabyte.sh ${YB_VERSION_TO_TEST_AGAINST} ${YUGABYTE_SRC}'
                    //}
                }
            }
        }
        stage('Parallel Build'){
            parallel {
                stage('Debezium Cache & Build'){
                    stages {
                        stage("Cache Debezium Dependencies") {
                            steps {
                                dir("${DEBEZIUM_CONNECTOR_HOME}") {
                                    cache (path: "$HOME/.m2/repository", key: "debezium-connector-${hashFiles('pom.xml')}") {
                                        sh 'mvn verify --fail-never -DskipTests -DskipITs'
                                    }
                                }
                            }
                        }
                        stage('Debezium Build') {
                            steps {
                                dir("${DEBEZIUM_CONNECTOR_HOME}") {
                                    script{
                                        env.DEBEZIUM_PKG_VERSION = sh(script: "mvn help:evaluate -Dexpression=project.version -q -DforceStdout", returnStdout: true).trim()
                                        sh 'mvn clean package -DskipTests -DskipITs'
                                    }
                                }
                            }
                        }
                    }
                }
                stage('cdcsdk Cache & Build'){
                    stages {
                        stage("Cache cdcsdk Dependencies") {
                            steps {
                                dir("${CDCSDK_SERVER_HOME}") {
                                    cache (path: "$HOME/.m2/repository", key: "cdcsdk-${hashFiles('pom.xml')}") {
                                        sh 'mvn verify --fail-never -DskipTests -DskipITs'
                                    }
                                }
                            }
                        }
                        stage('cdcsdk Build') {
                            steps {
                                script{
                                    dir("${CDCSDK_SERVER_HOME}") {
                                        env.CDCSDK_PKG_VERSION = sh(script: "mvn help:evaluate -Dexpression=project.version -q -DforceStdout", returnStdout: true).trim()
                                        sh './.github/scripts/build_cdcsdk.sh ${CDCSDK_PKG_VERSION}'
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        stage('Testing') {
            steps {
                withCredentials([
                    file(
                        credentialsId: '1854cb11-fa77-4b43-a2f9-aa8c038d30f0',
                        variable: 'GOOGLE_APPLICATION_CREDENTIALS'
                    ),
                    [
                        $class: 'AmazonWebServicesCredentialsBinding',
                        credentialsId: '28669ca6-7fb9-489d-82c4-be9fa7104d9b',
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                    ]
                ]) {
                    script{
                        dir("${CDCSDK_TESTING_HOME}") {
                            sh'''
                            mkdir -p ${CDCSDK_TESTING_HOME}/.aws
                            echo "[default]\naws_access_key_id = ${AWS_ACCESS_KEY_ID}\naws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}" | tee ${CDCSDK_TESTING_HOME}/.aws/credentials
                            '''
                            env.AWS_SHARED_CREDENTIALS_FILE="$CDCSDK_TESTING_HOME/.aws/credentials"
                            env.USERID = sh(script: "id -u", returnStdout: true).trim()
                            env.CDCSDK_SERVER_IMAGE="quay.io/yugabyte/cdcsdk-server:latest"
                            env.KAFKA_CONNECT_IMAGE="quay.io/yugabyte/debezium-connector:${DEBEZIUM_PKG_VERSION}"
                            sh 'mvn verify -Drun.releaseTests'
                        }
                    }
                }
            }
        }
        stage('Publish artifacts'){
            steps {
                script {
                    dir ("${CDCSDK_SERVER_HOME}/cdcsdk-server/cdcsdk-server-dist") {
                        if (params.PUBLISH_TO_S3) {
                            sh 'aws s3 cp --recursive --exclude="*" --include="*.gz" --include="*.gz.sha" --include="*.gz.md5" target ${RELEASE_BUCKET_PATH}/${CDCSDK_PKG_VERSION}'
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