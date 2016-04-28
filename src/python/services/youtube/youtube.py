# from src.python.services.lib.awsS3 import s3_upload
import datetime
import boto3
import os
import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumReader, DatumWriter
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import json



# Set DEVELOPER_KEY to the API key value from the APIs & auth > Registered apps
# tab of
#   https://cloud.google.com/console
# Please ensure that you have enabled the YouTube Data API for your project.
DEVELOPER_KEY = os.environ.get('GOOGLE_API_KEY')
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

S3_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')

print DEVELOPER_KEY
print S3_ACCESS_KEY
print S3_SECRET_KEY

def write_to_avro(data):
    schema = avro.schema.parse(open('./youtube.avsc').read())
    writer = DataFileWriter(open('./youtube.avro', 'w'), DatumWriter(),schema)
    for i in data:
        writer.append({"keyword": i})
    writer.close()


def youtube_search(options):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                    developerKey=DEVELOPER_KEY)
    # Call the search.list method to retrieve results matching the specified
    # query term.
    search_response = youtube.search().list(
            q=options.q,
            part="id,snippet",
            maxResults=options.max_results
    ).execute()
    videos = []
    channels = []
    playlists = []
    # Add each result to the appropriate list, and then display the lists of
    # matching videos, channels, and playlists.
    for search_result in search_response.get("items", []):
        if search_result["id"]["kind"] == "youtube#video":
            videos.append("%s (%s)" % (search_result["snippet"]["title"],
                                       search_result["id"]["videoId"]))
        elif search_result["id"]["kind"] == "youtube#channel":
            channels.append("%s (%s)" % (search_result["snippet"]["title"],
                                         search_result["id"]["channelId"]))
        elif search_result["id"]["kind"] == "youtube#playlist":
            playlists.append("%s (%s)" % (search_result["snippet"]["title"],
                                          search_result["id"]["playlistId"]))

    print "Videos:\n", "\n".join(videos), "\n"
    print "Channels:\n", "\n".join(channels), "\n"
    print "Playlists:\n", "\n".join(playlists), "\n"

    write_to_avro(videos)

    # with file('./youtube_keywords_{0}.csv'.format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S')), 'w') as f:
    #     for video in videos:
    #         f.write(video.encode('ascii', 'ignore') + '\n')

if __name__ == "__main__":


    # argparser.add_argument("--q", help="Search term", default="Google")
    argparser.add_argument("--q", help="Search term", default="iheartradio")
    argparser.add_argument("--max-results", help="Max results", default=25)
    args = argparser.parse_args()

    try:
        youtube_search(args)
    except HttpError, e:
        print "An HTTP error %d occurred:\n%s" % (e.resp.status, e.content)

    """
    TARGET_BASE_PATH = ''
    TARGET_BUCKET = 'jung-youtube-keywords'
    IHR_SESSION = boto3.Session(aws_access_key_id=S3_ACCESS_KEY,
                            aws_secret_access_key=S3_SECRET_KEY)
    s3_full_path = 'youtube_keywords_20160418_235335.csv'
    s3_upload('./youtube_keywords_20160418_235335.csv',
        s3_path=s3_full_path,
        bucket=TARGET_BUCKET,
        session=IHR_SESSION)
    """