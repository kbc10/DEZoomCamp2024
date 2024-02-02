import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Preprocessing: rows with zero passengers: ", data['passenger_count'].isin([0]).sum())
    print("Preprocessing: rows with zero trip distance: ", data['trip_distance'].isin([0]).sum())
    
    unique_vendors = data['VendorID'].unique()
    print("Unique Vendors: ", unique_vendors)

    data = data[data['trip_distance'] > 0]
    data = data[data['passenger_count'] > 0]
    
    print("After Preprocessing: rows with zero passengers: ", data['passenger_count'].isin([0]).sum())
    print("After Preprocessing: rows with zero trip distance: ", data['trip_distance'].isin([0]).sum())

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    unique_vendors = data['VendorID'].unique()
    print("Unique Vendors: ", unique_vendors)
    
    data.columns = (data.columns
                    .str.replace('ID','Id')
                    .str.replace('PU','pu')
                    .str.replace('DO','do')
    )
    
    for col in data.columns:
        new_name = ''.join(['_' + i.lower() if i.isupper() else i for i in col])
        new_name = ''.join(['_' + i.lower() if i.isupper() else i for i in col]).lstrip('_')
        print(new_name)
        data.rename(columns={col:new_name}, inplace=True)

    return data


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert 'vendor_id' in output.columns, 'vendor_id is not one of the columns'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers.'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance.'