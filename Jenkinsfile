pipeline {
    agent any
    environment {
        npm_config_cache = 'npm-cache'
        SERVERLESS_ACCESS_KEY = "${sh(script:'aws secretsmanager get-secret-value --secret-id SERVERLESS_ACCESS_KEY --query SecretString --output text | jq -r ".stage"', returnStdout: true).trim()}"
    }
    stages {
        stage('Dockerized deploy') {
            agent {
                docker {
                    image 'node:bullseye'
                    args "-e SERVERLESS_ACCESS_KEY -u root:root"
                    reuseNode true
                }
            }
            stages {
                stage('Install Dependencies') {
                    steps {
                        sh 'rm -rf .serverless*'
                        sh 'npm install -g serverless'
                        sh 'sls plugin install -n serverless-s3-remover'
                        sh 'sls plugin install -n serverless-iam-roles-per-function'
                        sh 'sls plugin install -n serverless-python-requirements'
                        sh 'apt update && apt install -y python3-pip'
                    }
                }
                stage("Deploy Serverless") {
                    steps {
                        sh "serverless deploy"
                    }
                }
            }
        }
    }
}
