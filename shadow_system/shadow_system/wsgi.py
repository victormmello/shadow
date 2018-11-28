"""
WSGI config for shadow_system project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.1/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'shadow_system.settings')

application = get_wsgi_application()

# add the hellodjango project path into the sys.path
# sys.path.append('/home/ec2-user/teste/shadow/shadow_system')
# add the virtualenv site-packages path to the sys.path
# sys.path.append('/home/ec2-user/eb-virt')
