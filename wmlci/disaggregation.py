"""
Disaggregate multifunctional processes
"""
"""
Disaggregate multifunctional processes
"""

from wmlci.log import log

def find_allocation_test_process(jsonld):
    # This function finds a relatively simple multifunctional process for testing purposes
    valid_methods = {"PHYSICAL_ALLOCATION", "ECONOMIC_ALLOCATION"}

    for pid, process in jsonld.data.get("processes", {}).items():
        exchanges = process.get("exchanges", [])
        allocation_method = process.get("defaultAllocationMethod")
        allocation_factors = process.get("allocationFactors", [])

        if 5 <= len(exchanges) <= 10 and len(allocation_factors) != 0 and allocation_method in valid_methods:
            log.info("Valid processes for testing disaggregation algorithm:")
            log.info(process.get("@id"))


def remove_nondefault_allocation_factors(jsonld):
    """
    Cleans the 'allocationFactors' list in each process of a Brightway JSON-LD dataset.

    This function iterates through all processes in the JSON-LD structure and removes any
    allocation factor entries whose 'allocationType' does not match the process's
    'defaultAllocationMethod'. The JSON-LD structure is modified in-place.

    Parameters:
    -----------
    jsonld : dict
        A dictionary representing the full Brightway JSON-LD dataset, expected to contain
        a 'processes' key with process dictionaries.

    Returns:
    --------
    dict
        The updated JSON-LD dictionary with cleaned allocation factors.
    """
    for process_id, process_data in jsonld.data['processes'].items():
        default_method = process_data.get('defaultAllocationMethod')
        if not default_method or "allocationFactors" not in process_data:
            continue
        cleaned_factors = [
            af for af in process_data["allocationFactors"]
            if af.get("allocationType") == default_method
        ]
        process_data["allocationFactors"] = cleaned_factors
    return jsonld

def get_multifunctional_processes(jsonld):
    """
    Identify multifunctional processes from a Brightway25 JSONLDImporter object.
    A process is considered multifunctional if it contains more than one product output.
    Product outputs are defined as exchanges where:
    - 'isInput' (or variants) is explicitly False
    - 'flow.flowType' is 'PRODUCT_FLOW' (case-insensitive)
    - 'flow.category' does NOT contain 'CUTOFF' or '56' (case-insensitive)
    """
    multifunctional_process_uuids = []

    for process_id, process_data in jsonld.data['processes'].items():
        products = []
        for exc in process_data.get('exchanges', []):
            # Normalize input flag detection
            input_flag = exc.get('isInput', exc.get('input', exc.get('IsInput', None)))
            if input_flag == True:
                continue

            flow = exc.get('flow', {})
            flow_type = flow.get('flowType', '').lower()
            category = flow.get('category', '').lower()
            flow_name = flow.get('name','').lower

            # Check for valid product flow and exclude CUTOFF or '56'
            if (flow_type == 'product_flow' and
                    'cutoff' not in category and
                    '56' not in category):
                products.append(exc)

        if len(products) > 1:
            log.info(f"{process_id} contains multifunctional processes that require further processing")
            multifunctional_process_uuids.append(process_id)

    return multifunctional_process_uuids



def validate_allocation_factors(importer, multifunctional_process_uuids, tolerance=0.01):
    """
    Validate allocation factors for multifunctional processes.

    This function checks that each product output in a multifunctional process:
        - Has an allocation factor.
        - Uses the process's default allocation method.
        - Has a value strictly between 0 and 1.
        - Contributes to a total allocation sum within a specified tolerance of 1.0.

    Parameters:
    -----------
    importer : JSONLDImporter
        A Brightway25 importer object containing parsed process data.

    multifunctional_process_uuids : list of str
        List of process UUIDs identified as multifunctional.

    tolerance : float, optional (default=0.01)
        Acceptable deviation from a total allocation sum of 1.0.

    Returns:
    --------
    tuple:
        - valid_processes : list of str
            UUIDs of processes that passed all allocation checks.
        - invalid_processes : dict
            Dictionary mapping process UUIDs to reasons for validation failure.
    """
    # todo: check bw method for 'input' vs 'IsInput' vs 'isInput'
    valid_processes = []
    invalid_processes = {}
    # Evaluate only multifunctional processes
    for process_id, process_data in importer.data['processes'].items():
        # print(process_id)
        if process_id not in multifunctional_process_uuids:
            continue

        # get default allocation method
        allocation_method = process_data.get('defaultAllocationMethod')
        # print(f"allocation method is {allocation_method}")
        # get allocation factors
        allocation_factors = process_data.get('allocationFactors', [])
        # print(allocation_factors)

        # Map allocation factors by product ID
        allocation_map = {
            af['product']['@id']: af for af in allocation_factors
            if af.get('allocationType') == allocation_method
        }
        # Get product flows
        products = [
            exc for exc in process_data.get('exchanges', [])
            if not exc.get('isInput') and
               exc.get('flow', {}).get('flowType') == 'PRODUCT_FLOW'
        ]

        # Check each allocation factor for each product
        reasons = []
        total_allocation = 0.0
        for product in products:
            product_id = product['flow']['@id']
            af = allocation_map.get(product_id)

            # Is one of the allocation factors missing
            if not af:
                log.info(f"Process {process_id}: Missing allocation factor for product {product_id}")
                continue
            # Is the allocation factor entry present but empty
            value = af.get('value')
            if value is None:
                log.info(f"Process {process_id}: Allocation factor for product {product_id} is None")
                continue
            # Is the value invalid
            if value <= 0 or value >= 1:
                log.info(f"Process {process_id}: Allocation factor for product {product_id} is {value}, must be between 0 and 1")
                continue

            total_allocation += value  # Sum of all allocation factors
        '''
        # Populate validation failures
        if reasons:
            invalid_processes[process_id] = reasons
            continue
        '''

        # Check that sum of allocation factors are within a reasonable tolerance of 1.0
        if abs(total_allocation - 1.0) > tolerance:
            log.info(f"Process {process_id}: Difference between AF sum and 1.0 is {total_allocation:.4f}, outside tolerance ±{tolerance}")
            continue

        valid_processes.append(process_id)

    return valid_processes


def disaggregate_multifunctional_processes(jsonld):
    """
    The purpose of this function is to disaggregate multifunctional processes.
    Each process needs to produce one product.
    The technosphere matrix needs to be square for running LCA calculations
    :param jsonld:
    :return jsonld:
    """
    jsonld = remove_nondefault_allocation_factors(jsonld)
    mf_processes = get_multifunctional_processes(jsonld)
    valid_processes = validate_allocation_factors(jsonld, mf_processes,tolerance=0.01)

    return jsonld