import os

# from jinja2 import Template
from pywps import get_ElementMakerForVersion, Service
from pywps.app.basic import get_xpath_ns
from pywps.tests import WpsClient, WpsTestResponse, client_for

VERSION = "1.0.0"
WPS, OWS = get_ElementMakerForVersion(VERSION)
xpath_ns = get_xpath_ns(VERSION)

TESTS_HOME = os.path.abspath(os.path.dirname(__file__))
# PYWPS_CFG = os.path.join(TESTS_HOME, 'pywps.cfg')


# def write_roocs_cfg():
#     cfg_templ = """
#     [project:cmip5]
#     base_dir = {{ base_dir }}/mini-esgf-data/test_data/badc/cmip5/data

#     [project:cmip6]
#     base_dir = {{ base_dir }}/mini-esgf-data/test_data/badc/cmip6/data

#     [project:cordex]
#     base_dir = {{ base_dir }}/mini-esgf-data/test_data/badc/cordex/data

#     [project:c3s-cmip5]
#     base_dir = {{ base_dir }}/mini-esgf-data/test_data/group_workspaces/jasmin2/cp4cds1/vol1/data

#     [project:c3s-cmip6]
#     base_dir = NOT DEFINED YET

#     [project:c3s-cordex]
#     base_dir = {{ base_dir }}/mini-esgf-data/test_data/group_workspaces/jasmin2/cp4cds1/vol1/data
#     """
#     cfg = Template(cfg_templ).render(base_dir=TESTS_HOME)


def resource_file(filepath):
    return os.path.join(TESTS_HOME, 'testdata', filepath)


class WpsTestClient(WpsClient):

    def get(self, *args, **kwargs):
        query = "?"
        for key, value in kwargs.items():
            query += "{0}={1}&".format(key, value)
        return super(WpsTestClient, self).get(query)


def wps_test_client_for(service):
    return WpsTestClient(service, WpsTestResponse)


def get_output(doc):
    """Copied from pywps/tests/test_execute.py.
    TODO: make this helper method public in pywps."""
    output = {}
    for output_el in xpath_ns(doc, '/wps:ExecuteResponse'
                                   '/wps:ProcessOutputs/wps:Output'):
        [identifier_el] = xpath_ns(output_el, './ows:Identifier')

        lit_el = xpath_ns(output_el, './wps:Data/wps:LiteralData')
        if lit_el != []:
            output[identifier_el.text] = lit_el[0].text

        ref_el = xpath_ns(output_el, './wps:Reference')
        if ref_el != []:
            output[identifier_el.text] = ref_el[0].attrib['href']

        data_el = xpath_ns(output_el, './wps:Data/wps:ComplexData')
        if data_el != []:
            output[identifier_el.text] = data_el[0].text

    return output


def run_with_inputs(process, datainputs):
    client = client_for(Service(processes=[process()]))
    url = (f"?service=WPS&request=Execute&version=1.0.0&identifier={process.__name__}"
           f"&datainputs={datainputs}")

    resp = client.get(url)
    return resp


def check_for_output_file(resp, filename):
    # output_url = resp.xml.findall('.//{http://www.opengis.net/wps/1.0.0}Reference')[0].get('href')
    output = get_output(resp.xml)
    assert os.path.basename(output['output']) == filename
