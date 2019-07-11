import scrapy
import os
import json
import csv
import re
from copy import copy

# Extract contributors data via GitHub v3 API from a given list of libraries
#
# The libraries csv must have a 'library' and a 'repo_name' field. If it contains a 'Repositories' field
# it is used instead and 'repo_name' is computed from every newline in the entry as a GitHub repository url.
#
# Usage:
# $ AUTH_TOKEN=... scrapy crawl top_contributors -a repos_filename=libraries.csv -o contributors.csv

class TopContributorsSpider(scrapy.Spider):
    name = "top_contributors"
    headers = {
        'Authorization': 'Bearer ' + os.environ['Github_1'],
    }

    TOP_CONTRIBUTORS = 5

    def load_libraries(self):
        csv_filename = self.repos_filename
        with open(csv_filename, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            libraries = []
            for row in reader:
                library_name = row['library']
                repositories = row.get('Repositories')

                if repositories:
                    for repository_urls in repositories.strip().splitlines():
                        libraries.append({
                            'library': library_name,
                            'repo_name': repository_urls.replace('https://github.com/', '').lower(),
                        })
                else:
                    libraries.append({
                        'library': library_name,
                        'repo_name': row['repo_name']
                    })

        return libraries

    def get_contributors_url(self, meta):
        return ('https://api.github.com/repos/{repo_name}/contributors?page=1&per_page=' + str(self.TOP_CONTRIBUTORS)).format_map(meta)

    def get_commits_list_url(self, meta):
        new_meta = copy(meta)
        new_meta['author'] = meta['login']
        return ('https://api.github.com/repos/{repo_name}/commits?author={author}&page=1&per_page=1').format_map(new_meta)

    # Contributors we don't care to get emails from:
    # * username@users.noreply.github.com (https://help.github.com/articles/about-commit-email-addresses/)
    # * name: 'GitHub', email: noreply@github.com (Somme committer entries look like that)
    def contributor_is_valid(self, contributor):
        return  contributor and \
                contributor.get('email') and \
                not re.search(r'@users\.noreply\.github\.com$', contributor['email'], re.IGNORECASE) and \
                contributor['email'] != 'noreply@github.com'

    def start_requests(self):
        libraries = self.load_libraries()
        for library in libraries:
            yield scrapy.Request(self.get_contributors_url(library), headers=self.headers, callback=self.parse_contributors, meta=library)

    def parse_contributors(self, response):
        meta = response.meta
        contributors = json.loads(response.text)
        for contributor in contributors:
            meta['login'] = contributor['login']
            yield scrapy.Request(self.get_commits_list_url(meta), headers=self.headers, callback=self.parse_commits, meta=meta)

    def parse_commits(self, response):
        meta = response.meta
        commits = json.loads(response.text)
        contributors = []
        for commit_entry in commits:
            commit = commit_entry['commit']
            author = commit.get('author')
            committer = commit.get('committer')
            if self.contributor_is_valid(author):
                contributors.append({
                    'library': meta['library'],
                    'repo_name': meta['repo_name'],
                    'login': meta['login'],
                    'name': author.get('name'),
                    'email': author['email'],
                })

            if self.contributor_is_valid(committer) and committer['email'] != author['email']:
                contributors.append({
                    'library': meta['library'],
                    'repo_name': meta['repo_name'],
                    'name': committer['name'],
                    'email': committer['email'],
                })

        return contributors
