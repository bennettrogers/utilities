import boto
from boto.ec2.autoscale import LaunchConfiguration, AutoScalingGroup, Tag

as_conn = boto.connect_autoscale()
elb_conn = boto.connect_elb()

AUTOSCALE_CONFIGS = [
        #{
        #    'tier':'',
        #    'lifecycle':'',
        #    'instance_size':'',
        #    'image_id':'',
        #    'capacity':0,
        #    'key_pair':'',
        #    'availability_zones':[],
        #    'security_groups':[],
        #    'tags': {
        #        'Name':'',
        #        'tier':'',
        #        'role':'',
        #    },
        #},
]

for config in AUTOSCALE_CONFIGS:

    resource_name = '{0}_{1}'.format(config['tier'], config['lifecycle'],)

    lc_args = {
            'name':resource_name,
            'image_id':config['image_id'],
            'key_name':config['key_pair'],
            'security_groups':config['security_groups'],
            'instance_type':config['instance_size'],
            'spot_price': config['price'] if config['lifecycle'] is 'spot' else None,
            }

    # delete the launch configuration if it already exists (TODO: figure out if we can update existing configs)
    if as_conn.get_all_launch_configurations(names=[resource_name]):
        as_conn.delete_launch_configuration(launch_config_name=resource_name)

    lc = LaunchConfiguration(**lc_args)
    as_conn.create_launch_configuration(lc)
    lc_obj = as_conn.get_all_launch_configurations(names=[resource_name])[0]

    tags = config['tags']
    as_tags = [Tag(key=key, value=value, propagate_at_launch=True, resource_id=resource_name) for key, value in tags.items()]

    asg_args = {
            'group_name':resource_name,
            'availability_zones':config['availability_zones'],
            'launch_config':lc_obj,
            'min_size':config['capacity'],
            'max_size':config['capacity'],
            'tags':as_tags,
            }

    # delete the asg if it already exists (TODO: figure out if we can update existing asgs)
    if as_conn.get_all_groups(names=[resource_name]):
        as_conn.delete_auto_scaling_group(name=resource_name)

    asg = AutoScalingGroup(**asg_args)
    as_conn.create_auto_scaling_group(asg)

#==================================================
#as_conn.delete_launch_configuration(launch_config_name=resource_name)
