import json
import time
from datetime import date

from selenium import webdriver
from bs4 import BeautifulSoup
import requests
from selenium.common.exceptions import NoSuchElementException

from prefect import task


@task
def get_jobs_links(url):
    driver = webdriver.PhantomJS()
    driver.get(url)
    time.sleep(5)
    job_links = []
    content = driver.page_source
    soup = BeautifulSoup(content, features="html.parser")
    for element in soup.findAll(attrs={'class': 'title'}):
        name = element.find('a')
        job_link = name.attrs['href']
        job_links.append(job_link)
    driver.close()
    return job_links

@task
def read_job(job: str):
    job = job.replace('/gulp2/g/projekte/', 'https://www.gulp.de/gulp2/rest/internal/projects/')
    if '/extern/' in job:
        job = job.replace('/extern/', '/external/')
    if '/agentur/' in job:
        job = job.replace('/agentur/', '/agency/')
    response = requests.get(job)
    return response.json()

@task
def get_pages(url: str):
    driver = webdriver.PhantomJS()
    driver.get(url=url)
    pages = [driver.current_url]
    try:
        next_button = driver.find_element_by_class_name('next')
    except NoSuchElementException:
        driver.save_screenshot('screenshot.png')
    continued = True
    while continued:
        next_button.click()
        next_button = driver.find_element_by_class_name('next')
        if pages[-1] == driver.current_url:
            break
        pages.append(driver.current_url)
    return pages

@task
def write(data: list):
    import json
    import boto3    
    s3 = boto3.resource('s3')
    s3object = s3.Object('testmydask', f'gulp-{date.today().strftime("%Y-%m-%d")}.json')
    s3object.put(
        Body=(bytes(json.dumps(data).encode('UTF-8')))
    )


from prefect import Flow, flatten

with Flow("gulp-prefect-dask") as flow:
    pages = get_pages('https://www.gulp.de/gulp2/g/projekte?order=DATE_DESC')
    jobs = get_jobs_links.map(pages)
    data = read_job.map(flatten(jobs))
    write(data)
