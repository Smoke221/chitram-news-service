module.exports = {
  apps: [{
    name: "chitram-news-service",
    script: "./main_service.py",
    interpreter: "python3",
    watch: false,
    env: {
      NODE_ENV: "production"
    }
  }]
}
