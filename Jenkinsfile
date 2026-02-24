#!/usr/bin/env groovy
pipeline {
  agent {
      label 'commonagent'
  }
  environment {
    JAVA_HOME = '/usr/lib/jvm/openjdk-jdk-21.0.1+12-LTS'
    PATH = "/usr/lib/jvm/openjdk-jdk-21.0.1+12-LTS/bin:${env.PATH}"
    JDK_FILE_NAME = 'openjdk-jdk-21.0.1+12-LTS.tgz'
    JAVA_VERSION = '21.0.1_12'
  }

  stages {
    stage('Build artefact') {
      steps {
        ansiColor('xterm') {
        	sh('make build')
        }
      }
    }
    stage('Upload to s3') {
      steps {
        sh("""
           make push-s3 S3_BUCKET=txm-lambda-functions-integration
           make push-s3 S3_BUCKET=txm-lambda-functions-qa
           make push-s3 S3_BUCKET=txm-lambda-functions-staging
           make push-s3 S3_BUCKET=txm-lambda-functions-production
           """)
      }
    }
    stage ('Run cip-attrep-terraform job') {
      steps {
        build(job: 'cir/terraform/attrep/build-all-environments', wait: false)
      }
    }
  }
}
