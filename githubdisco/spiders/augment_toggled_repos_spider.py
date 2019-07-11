import scrapy
import os
import json
import csv
import time
import re
from calendar import timegm

# Extract agumented info for toggled repositories via GitHub v3 API
#
# Usage:
# $ AUTH_TOKEN=... scrapy crawl augment_toggled_repos -a repos_filename=repositories.csv -o ../results/raw/results-augmented-data-`date -u "+%Y%m%d%H%M%S"`.csv

class AugmentToggledReposSpider(scrapy.Spider):

    name = "augment_toggled_repos"
    handle_httpstatus_list = [404]
    headers = {
        'Authorization': 'Bearer ' + os.environ['Github_1'],
    }
    augmented = {}
    max_items_per_page = 100

    def get_contributors_url(self, meta):
        return ('https://api.github.com/repos/{repo_name}/contributors?anon=1&page={page}&per_page=' + str(self.max_items_per_page)).format_map(meta)

    def get_commits_list_url(self, meta):
        return ('https://api.github.com/repos/{repo_name}/commits?page={page}&per_page=1').format_map(meta)

    def start_requests(self):
        toggled_repos = self.load_toggled_repos()
        for repo in toggled_repos:
            repo_name = repo['repo_name']
            self.augmented[repo_name] = {
                'repo_name': repo_name,
                '___stage___': 0
            }
            meta = { 'repo_name': repo_name, 'page': 1 }
            repo_info_url = 'https://api.github.com/repos/{0}'.format(repo_name)
            yield scrapy.Request(url=repo_info_url, headers=self.headers, callback=self.parse_repo_info, meta=meta)
            yield scrapy.Request(self.get_contributors_url(meta), headers=self.headers, callback=self.parse_contributors, meta=meta)
            yield scrapy.Request(self.get_commits_list_url(meta), headers=self.headers, callback=self.parse_first_commit, meta=meta)

    def load_toggled_repos(self):
        csv_filename = self.repos_filename
        toggled_repos = []
        with open(csv_filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                toggled_repos.append(row)

        return toggled_repos

    def as_epoch(self, json_timestamp):
        return timegm(time.strptime(json_timestamp, "%Y-%m-%dT%H:%M:%SZ"))

    def parse_repo_info(self, response):
        repo_name = response.meta['repo_name']
        augmented_data = self.augmented[repo_name]

        if response.status == 404:
            yield self.handle_404(augmented_data)
            return

        json_response = json.loads(response.text)

        augmented_data['size_bytes'] = json_response['size'] * 1024
        augmented_data['forked_from'] = json_response['source']['full_name'] if json_response.get('source') else None
        augmented_data['last_commit_ts'] = self.as_epoch(json_response['pushed_at'])
        augmented_data['created_at'] = self.as_epoch(json_response['created_at'])
        augmented_data['language'] = json_response['language']
        augmented_data['___stage___'] += 1

        yield self.augmented_complete(augmented_data)

    def parse_contributors(self, response):
        meta = response.meta
        repo_name = meta['repo_name']
        augmented_data = self.augmented[repo_name]

        if response.status == 404:
            yield self.handle_404(augmented_data)
            return

        json_response = json.loads(response.text)

        if meta['page'] == 1:
            augmented_data['number_of_contributors'] = 0
            augmented_data['number_of_commits'] = 0

        contributors = len(json_response)
        if contributors > 0:
            augmented_data['number_of_contributors'] += contributors
            augmented_data['number_of_commits'] += sum(contributor['contributions'] for contributor in json_response)

        if contributors == self.max_items_per_page:
            meta['page'] += 1
            yield scrapy.Request(self.get_contributors_url(meta), headers=self.headers, callback=self.parse_contributors, meta=meta)

        if contributors == 0 or contributors < self.max_items_per_page:
            augmented_data['___stage___'] += 1
            yield self.augmented_complete(augmented_data)

    def get_last_page_from_header(self, response):
        link_header = response.headers.get('Link')
        rel_links = link_header.decode('utf-8').split(', ')
        for link, rel in [rel_link.split('; ') for rel_link in rel_links]:
            if rel == 'rel="last"':
                last_page = int(re.search(r'page=(\d+)', link).group(1))
                return last_page

    def parse_first_commit(self, response):
        meta = response.meta
        repo_name = meta['repo_name']
        augmented_data = self.augmented[repo_name]

        if response.status == 404:
            yield self.handle_404(augmented_data)
            return

        last_page = self.get_last_page_from_header(response)
        if meta['page'] == last_page:
            json_response = json.loads(response.text)
            augmented_data['first_commit_sha'] = json_response[0]['sha']
            augmented_data['___stage___'] += 1
            yield self.augmented_complete(augmented_data)
        elif meta['page'] == 1:
            meta['page'] = last_page
            yield scrapy.Request(self.get_commits_list_url(meta), headers=self.headers, callback=self.parse_first_commit, meta=meta)

    def handle_404(self, augmented_data):
        augmented_data['___stage___'] += 1
        augmented_data['repo_not_found'] = True
        return self.augmented_complete(augmented_data)

    def augmented_complete(self, augmented_data):
        if augmented_data['___stage___'] == 3:
            del augmented_data['___stage___']
            return augmented_data