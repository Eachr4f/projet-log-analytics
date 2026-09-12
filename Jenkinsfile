pipeline {
    agent any

    environment {
        KUBECONFIG = '/var/jenkins_home/.kube/config'
    }

    stages {
        stage('Lint Python') {
            steps {
                sh '''
                    flake8 generator/generator.py --max-line-length=120 || true
                    flake8 alerting/alert_consumer.py --max-line-length=120 || true
                '''
            }
        }

        stage('Validate Terraform') {
            steps {
                dir('terraform') {
                    sh 'terraform init -backend=false || echo "Terraform non installé sur cet agent"'
                    sh 'terraform validate || true'
                }
            }
        }

        stage('Deploy Kafka Topics to K8s') {
            steps {
                sh 'kubectl --insecure-skip-tls-verify=true apply -f k8s/kafka-nodepool.yaml'
                sh 'kubectl --insecure-skip-tls-verify=true apply -f k8s/kafka-cluster.yaml'
                sh 'kubectl --insecure-skip-tls-verify=true apply -f k8s/topic-logs-raw.yaml'
                sh 'kubectl --insecure-skip-tls-verify=true apply -f k8s/topic-logs-anomalies.yaml'
                sh 'kubectl --insecure-skip-tls-verify=true get kafkatopics -n kafka'
            }
        }
    }

    post {
        success {
            echo 'Pipeline terminé avec succès'
        }
        failure {
            echo 'Le pipeline a échoué'
        }
    }
}
