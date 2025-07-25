
from wmlci.wmlci_log import log

##############################################
### Disaggregate multifunctional processes ###
##############################################

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


def get_multifunctional_processes(jsonld):
    """
    Identify multifunctional processes from a Brightway25 JSONLDImporter object.
    A process is considered multifunctional if it contains more than one product output.
    Product outputs are defined as exchanges where:
    - 'input' is False
    - 'flow.flowType' is 'PRODUCT_FLOW'
    """
    multifunctional_process_uuids = []

    # Create a list of products for current process
    for process in jsonld.data['processes']:
        products = [
            exc for exc in process.get('exchanges', [])
            if not exc.get('input') and exc.get('flow', {}).get('flowType') == 'PRODUCT_FLOW'
        ]
        # Go to next process if not multifunctional
        if len(products) > 1:
            multifunctional_process_uuids.append(process.get('@id'))

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
    valid_processes = []
    invalid_processes = {}
    # Evaluate only multifunctional processes
    for process in importer.data['processes']:
        process_id = process.get('@id')
        if process_id not in multifunctional_process_uuids:
            continue

        allocation_method = process.get('defaultAllocationMethod')  # get default allocation method
        allocation_factors = process.get('allocationFactors', [])  # get allocation factors

        # Map allocation factors by product ID
        allocation_map = {
            af['product']['@id']: af for af in allocation_factors
            if af.get('allocationType') == allocation_method
        }
        # Get product flows
        products = [
            exc for exc in process.get('exchanges', [])
            if not exc.get('input') and exc.get('flow', {}).get('flowType') == 'PRODUCT_FLOW'
        ]

        # Check each allocation factor for each product
        reasons = []
        total_allocation = 0.0
        for product in products:
            product_id = product['flow']['@id']
            af = allocation_map.get(product_id)

            # Is one of the allocation factors missing
            if not af:
                reasons.append(f"Process {process_id}: Missing allocation factor for product {product_id}")
                continue
            # Is the allocation factor entry present but empty
            value = af.get('value')
            if value is None:
                reasons.append(f"Process {process_id}: Allocation factor for product {product_id} is None")
                continue
            # Is the value invalid
            if value <= 0 or value >= 1:
                reasons.append(
                    f"Process {process_id}: Allocation factor for product {product_id} is {value}, must be between 0 and 1")
                continue

            total_allocation += value  # Sum of all allocation factors

        # Populate validation failures
        if reasons:
            invalid_processes[process_id] = reasons
            continue

        # Check that sum of allocation factors are within a reasonable tolerance of 1.0
        if abs(total_allocation - 1.0) > tolerance:
            invalid_processes[process_id] = [
                f"Process {process_id}: Total allocation sum is {total_allocation:.4f}, outside tolerance ±{tolerance}"
            ]
            continue

        valid_processes.append(process_id)

    return valid_processes, invalid_processes


def disaggregate_multifunctional_processes(jsonld):
    """
    The purpose of this function is to disaggregate multifunctional processes.
    Each process needs to produce one product.
    The technosphere matrix needs to be square for running LCA calculations
    :param jsonld:
    :return jsnold:
    """
    mf_processes = get_multifunctional_processes(jsonld)
    valid_processes, invalid_processes = validate_allocation_factors(jsonld, mf_processes,tolerance=0.01)


    return jsonld