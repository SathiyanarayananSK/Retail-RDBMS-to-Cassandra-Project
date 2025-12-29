from scripts.tools.connections import AstraDBConnection


retail_astra = AstraDBConnection()
retail_astra.connect_to_the_cluster()