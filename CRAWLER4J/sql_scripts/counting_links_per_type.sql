select count(edges.source) from edges, nodes 
                           where edges.target = nodes.id and 
                                 nodes.status_code=404 and 
                                 nodes.page_type='ListeProduitFiltre' 