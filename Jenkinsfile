pipeline {

    agent {
            docker {
                image 'hseeberger/docker-sbt'
            }
        }

    stages {

        stage('Compile') {
            steps {
                echo "Compiling..."
                sh "sbt compile"
            }
        }

        stage('Test') {
            steps {
                echo "Testing..."
                sh "sbt test"
            }
        }
    }

}