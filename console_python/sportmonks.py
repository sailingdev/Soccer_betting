#!/usr/bin/python
import requests
import requests_cache
import json

api_token = ''
api_url = 'https://soccer.sportmonks.com/api/v2.0/'

def init(token):
    """Before you are able to make requests to our API it's required that you have created an Account and API token. You can create API tokens via the settings page which is available when you are logged in. API token will only be shown to you when you create them. Please make sure that you store your token safely.

    To authorize request to our API you must add a parameter to your request called api_token. The value of this parameter is the actual token you received when you created it. SportMonks will measure the usage of all the tokens you have generated and will make them visible via your dashboard."""

    global api_token
    api_token = token

def get(endpoint, include=None, leagues = None,page=None, paginated=True):
    payload = {'api_token': api_token }
    if leagues:
        payload['leagues'] = leagues
    if include:
        payload['include'] = include
    if page:
        paginated = True
        payload['page'] = page
    r = requests.get(api_url + endpoint, params=payload)
    parts = json.loads(r.text)
    data = parts.get('data')
    meta = parts.get('meta')
    if not data:
        return None
    pagination = meta.get('pagination')
    if pagination:
        pages = int(pagination['total_pages'])
    else:
        pages = 1
    if (not paginated) and (pages > 1):
        for i in range(2, pages+1):
            payload['page'] = i
            r = requests.get(api_url + endpoint, params=payload)
            next_parts = json.loads(r.text)
            next_data = next_parts.get('data')
            if next_data:
                data.extend(next_data)
    return data

def continents():
    """With this endpoint you are able to retrieve a list of continents."""
    return get('continents')

def continent(id):
    """With this endpoint you are able to retrieve details a specific continent."""
    return get('continents/{}'.format(id))

def countries():
    """With this endpoint you are able to retrieve a list of countries."""
    return get('countries')

def country(id):
    """With this endpoint you are able to retrieve details a specific country."""
    return get('countries/{}'.format(id))

def leagues():
    """With this endpoint you are able to retrieve a list of leagues."""
    return get('leagues')

def league(id):
    """With this endpoint you are able to retrieve details a specific league."""
    return get('leagues/{}'.format(id))

def seasons():
    """With this endpoint you are able to retrieve a list of seasons."""
    return get('seasons')

def season(id):
    """With this endpoint you are able to retrieve a specific season."""
    return get('seasons/{}'.format(id))

def fixtures(first, last=None, include=None, page=None, paginated=True):
    """With this endpoint you are able to retrieve all fixtures between 2 dates or retrieve all fixtures for a given date."""
    if last is None:
        return get('fixtures/date/{}/'.format(first), include, page, paginated)
    else:
        return get('fixtures/between/{}/{}/'.format(first, last), include, page, paginated)

def fixture(id):
    """With this endpoint you are able to retrieve a fixture by it's id. """
    return get('fixtures/{}'.format(id))

def todayscores():
    """With this endpoint you are able to retrieve all fixtures that are played on the current day."""
    return get('livescores')

def livescores(include=None, page=None, paginated=True):
    """With this endpoint you are able to retrieve all fixtures for are currently beeing played. This response will also contain games that are starting within 45 minutes and that are ended less then 30 minutes ago."""
    return get('livescores/now', include, page, paginated)

def standings(season):
    """With this endpoint you are able to retrieve the standings for a given season."""
    return get('standings/season/{}'.format(season))

def venue(id):
    """With this endpoint you can get more information about a venue."""
    return get('venues/{}'.format(id))

def teams(season):
    """It might be interesting to know what teams have played a game in a partisucal season. with this endpoint you are able to retrieve a list of teams that have at least played 1 game in it."""
    return get('teams/season/{}'.format(season))

def team(id):
    """With this endpoint you are able to retrieve basic team information. """
    return get('teams/{}'.format(id))

def rounds(season):
    """With this endpoint you are able to retrieve all rounds for a given season (if applicable)."""
    return get('rounds/season/{}'.format(season))

def round(id):
    """With this endpoint you are able to retrieve a round by a given id."""
    return get('rounds/{}'.format(id))

def get_odds_by_fixture(fixture_id):
    return get('odds/fixture/{}'.format(fixture_id))

def get_flatodds_by_fixture(fixture_id):
    return get('flatodds/fixture/{}'.format(fixture_id))

def bookmakers():
    return get('bookmakers')

def get_prematch_odds_by_fixture_bookmakers(fixture_id, bookmaker_id):
    return get('odds/fixture/' + fixture_id + '/bookmaker/' + bookmaker_id)

def get_Inplayodds_by_Fixture(fixture_id):
    return get('odds/inplay/fixture/{}'.format(fixture_id))

# Use cache
#requests_cache.install_cache('sportmonks_cache', backend='sqlite', expire_after=24*3600)
