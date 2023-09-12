from typing import List, Dict, Set, Optional

map = {
    'vladimir.prus' : 'vladimir',
    'espander-joom' : 'espander',
    'g00sek' : 'gusev',
    'dmgburg' : 'gburg',
    'justakir' : 'yatsushko',
    'mshlyapenko-joom' : 'mshlyapenko',
    'NikitaBragin' : 'nbragin',
    'anyazay' : 'annzaychik',
    'Zhabrevalexander' : 'zhabrev',
    'irinatroyanovskaya' : 'troyanovskaya',
    '5irinatroyanovskaya' : 'troyanovskaya',
    'ilypavlov' : 'ilypavlov',
    'NastiaMitiushkina' : 'amitiushkina',
    'lexx-a' : 'yushkov',
    'Eleonid' : 'leonid.enov',
    'eleon16' : 'leonid.enov',
    'marie.balatskaya' : 'leonid.enov',
    'maria' : 'leonid.enov',
    'akarpoff' : 'a.vilman',
    'akarpov' : 'a.vilman',
    'profunctor' : 'aplotnikov',
    'logrel87' : 'logrel',
    'alina2703' : 'acherezova',
    'alinacherez' : 'acherezova',
    'garkavaya' : 'leonid.enov',
    'adelli-am' : 'leonid.enov',
    'joom-adelina' : 'leonid.enov',
    'EKutynina' : 'ekutynina',
    'msafonov' : 'gusev',
    'evstviaJoom' : 'evstvia',
    'misha-kh' : 'troyanovskaya',
    'rashonomnom' : 'marksysoev',
    'alexvi88' : 'a.vilman',
    'EvilMary' : 'kostjabobkov',
    'moriture' : 'e.kotsegubov',
    'lockyStar' : 'zhabrev',
    'ktitova' : 'ilypavlov',
    'BartolomeuD' : 'marksysoev'
}

def normalize(user: str):
    if "+" in user:
        user = user.split("+")[1]
    return map.get(user, user)


def find_models(manifest_file) -> List[str]:
    import json
    manifest = json.load(manifest_file)

    print('Models with no owner:')
    for (key, node) in manifest['nodes'].items():
        if node['resource_type'] in ['model', 'snapshot']:
            if 'model_owner' not in node['meta']:
                import subprocess
                filename = node['original_file_path']
                command = f"git log --follow {filename} | grep Author: | cut -d' ' -f2- | cut -d'<' -f2- | cut -d'@' -f 1"
                all = set([normalize(user) for user in subprocess.getoutput(f"{command} | sort | uniq").split("\n")])

                print(filename)
                if 'models/spark/b2b_mart' in filename and 'amitiushkina' in all:
                    all = {'amitiushkina'}
                if 'snapshots/spark/b2b_mart' in filename and 'amitiushkina' in all:
                    all = {'amitiushkina'}
                if 'anomaly_detection' in filename and 'logrel' in all:
                    all = {'logrel'}
                if len(all) > 1 and 'yushkov' in all:
                    all.remove('yushkov')
                if len(all) > 1 and 'gburg' in all:
                    all.remove('gburg')
                if len(all) > 1 and 'aplotnikov' in all:
                    all.remove('aplotnikov')
                if len(all) > 1 and 'espander' in all:
                    all.remove('espander')
                if len(all) > 1 and 'gusev' in all:
                    all.remove('gusev')
                if len(all) > 1 and '>' in all:
                    all.remove('>')
                if len(all) > 1:
                    print(all)
                    continue

                last = list(all)[0]

                from pathlib import Path
                data = Path(filename).read_text()
                if "meta = {" in data:
                    data = data.replace("meta = {", "meta = {\n      'model_owner' : '@%s'," % last)
                    with open(filename, 'w') as file:
                        file.write(data)
                        file.close()
                        pass
                elif "config(" in data:
                    data = data.replace("config(", """config(
    meta = {
      'model_owner' : '@%s'
    },""" % last)
                    with open(filename, 'w') as file:
                        file.write(data)
                        file.close()
                        pass
                elif "config(" not in data:
                    data = """{{
  config(
    meta = {
      'model_owner' : '@%s'
    },
  )
}}

%s""" % (last, data)
                    with open(filename, 'w') as file:
                        file.write(data)
                        file.close()
                        pass



if __name__ == '__main__':
    with open("target/manifest.json") as file:

        find_models(file)
