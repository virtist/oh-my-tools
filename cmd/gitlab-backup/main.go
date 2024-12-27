package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// GitLabProject represents a GitLab project from the API response
type GitLabProject struct {
	ID                int       `json:"id"`
	Name              string    `json:"name"`
	PathWithNamespace string    `json:"path_with_namespace"`
	SSHURLToRepo      string    `json:"ssh_url_to_repo"`
	HTTPURLToRepo     string    `json:"http_url_to_repo"`
	LastActivity      time.Time `json:"last_activity_at"`
}

// Configuration structure for reading from config file
type Config struct {
	Token     string `mapstructure:"token"`
	GitlabURL string `mapstructure:"gitlab_url"`
	BackupDir string `mapstructure:"backup_dir"`
	MaxJobs   int    `mapstructure:"max_jobs"`
}

// fetchProjects retrieves all accessible GitLab projects with pagination
func fetchProjects(gitlabURL, token string) ([]GitLabProject, error) {
	var projects []GitLabProject
	page := 1

	// Create an HTTP client that skips HTTPS certificate validation
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // Skip certificate validation
		},
		Timeout: 10 * time.Second,
	}

	for {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v4/projects?membership=true&&simple=true&per_page=100&page=%d", gitlabURL, page), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("PRIVATE-TOKEN", token)

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to fetch projects: %s", resp.Status)
		}

		var pageProjects []GitLabProject
		if err := json.NewDecoder(resp.Body).Decode(&pageProjects); err != nil {
			return nil, err
		}

		if len(pageProjects) == 0 {
			break // No more projects to fetch
		}

		projects = append(projects, pageProjects...)
		page++
	}
	return projects, nil
}

// backupGitRepo clones or updates a GitLab project with retry mechanism
func backupGitRepo(repoURL, backupDir string, wg *sync.WaitGroup, sem chan struct{}, retries int) {
	defer wg.Done()
	repoName := filepath.Base(repoURL)
	repoPath := filepath.Join(backupDir, repoName)

	for i := 0; i <= retries; i++ {
		select {
		case sem <- struct{}{}: // Acquire semaphore to control concurrency
			if _, err := os.Stat(repoPath); os.IsNotExist(err) {
				// Clone if repo does not exist
				os.MkdirAll(backupDir, 0755)
				logrus.Infof("Cloning %s to %s...\n", repoName, repoPath)
				cmd := exec.Command("git", "clone", "--bare", repoURL, repoPath)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				if err := cmd.Run(); err != nil {
					logrus.Errorf("Failed to clone %s: %v", repoName, err)
					<-sem
					continue // Retry
				}
			} else {
				// Feach
				logrus.Infof("Updating %s in %s...\n", repoName, repoPath)
				cmd := exec.Command("git", "-C", repoPath, "fetch", "--all")
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				if err := cmd.Run(); err != nil {
					logrus.Errorf("Failed to fetch %s: %v", repoName, err)
					<-sem
					continue // Retry
				}
			}
			<-sem // Release semaphore
			break
		}
	}
}

// lastBackupTime reads the last backup time from a file
func lastBackupTime(filePath string) (time.Time, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, string(data))
}

// saveLastBackupTime saves the last backup time to a file
func saveLastBackupTime(filePath string) error {
	return os.WriteFile(filePath, []byte(time.Now().Format(time.RFC3339)), 0644)
}

// readConfig reads configuration from a YAML file
func readConfig(configFile string) (*Config, error) {
	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}
	if config.Token == "" || config.GitlabURL == "" || config.BackupDir == "" {
		return nil, errors.New("missing required configuration fields")
	}
	return &config, nil
}

func printBackupSummary(projects []GitLabProject) {
	logrus.Infof("Found %d projects:\n", len(projects))
	for _, project := range projects {
		logrus.Infof("- %s (%s) @%s\n", project.Name, project.PathWithNamespace, project.LastActivity)
	}
}

func backupAllProjects(config *Config, lastBackupFile string, retries int) {

	// Fetch all accessible GitLab projects
	projects, err := fetchProjects(config.GitlabURL, config.Token)
	if err != nil {
		logrus.Fatalf("Error fetching projects: %v", err)
	}

	// Create backup directory if it doesn't exist
	if _, err := os.Stat(config.BackupDir); os.IsNotExist(err) {
		if err := os.MkdirAll(config.BackupDir, 0755); err != nil {
			logrus.Fatalf("Error creating backup directory: %v", err)
		}
	}

	// Read the last backup time
	var lastBackup time.Time
	if _, err := os.Stat(lastBackupFile); err == nil {
		lastBackup, err = lastBackupTime(lastBackupFile)
		if err != nil {
			logrus.Fatalf("Error reading last backup time: %v", err)
		}
	}

	printBackupSummary(projects)

	// Initialize concurrency control
	var wg sync.WaitGroup
	sem := make(chan struct{}, config.MaxJobs) // Limit to max concurrent jobs

	// Backup each project concurrently with retry and concurrency control
	for _, project := range projects {
		if project.LastActivity.After(lastBackup) {
			wg.Add(1)

			namespaceDir := filepath.Dir(project.PathWithNamespace)
			backupDir := path.Join(config.BackupDir, namespaceDir)

			go backupGitRepo(project.HTTPURLToRepo, backupDir, &wg, sem, retries)
		} else {
			logrus.Infof("Skipping %s, no updates since last backup.\n", project.Name)
		}
	}

	wg.Wait()
}

func main() {
	// Parse command line flags
	configFile := flag.String("config", "config.yaml", "Path to the configuration file")
	lastBackupFile := flag.String("last-backup", "last_backup.txt", "File to store the last backup time")
	retries := flag.Int("retries", 3, "Number of retries for backup operations")
	flag.Parse()

	// Read configuration
	config, err := readConfig(*configFile)
	if err != nil {
		logrus.Fatalf("Error reading config file: %v", err)
	}

	backupAllProjects(config, *lastBackupFile, *retries)

	// Save the last backup time
	if err := saveLastBackupTime(*lastBackupFile); err != nil {
		logrus.Fatalf("Error saving last backup time: %v", err)
	}
}
