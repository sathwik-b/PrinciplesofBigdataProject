from django.shortcuts import render
from subprocess import run, PIPE
import sys


# Create your views here.
def home(request):
    return render(request, 'twitterdata/home.html')

def runscript(request, id):
    column_index = {
        "1": "retweet",
        "2": "statusescount",
        "3": "language",
        "4": "place",
        "5": "followerscount",
        "6": "location",
        "7": "date",
        "8": "hashtags",
        "9": "keywords",
        "10": "year"
        
    }
    print("in vide")
    output = run([sys.executable, 'test.py', id], shell=False, stdout=PIPE)
    print("after")
    print(output)
    # foldername = 'query' + id
    # rootdir = 'outs/' + foldername
    # print(rootdir)
    # for subdir, dirs, files in os.walk(rootdir):
    #     for file in files:
    #         ext = os.path.splitext(file)[-1].lower()
    #         if (ext == '.csv'):
    #             filepath = rootdir+'/' + file
    # queryOutput = pd.read_csv(filepath)
    # result = queryOutput.set_index(column_index.get(id)).to_dict().get('count')
    image_path= '/static/images/query'+id+'.png'
    return render(request, 'twitterdata/output.html', {'image_path': image_path})
