import apache_beam as beam
import json
import logging
import vertexai
from vertexai.language_models import TextEmbeddingModel


class GenerateEmbedding(beam.DoFn):

    def __init__(self, project_id, region, model_name):
        self.project_id = project_id
        self.region     = region
        self.model_name = model_name
        self._model     = None

    def setup(self): #Beam calls the setup() method once per worker to initialize it. 
        #We initialize the vertex AI client once per worker too because it is very costly (and we dont have money). If we initialize is in the process() method, it would be initialized once per element in the PCollection.
  
        #We initialize te vertex AI client and define region and project.
        vertexai.init(project=self.project_id, location=self.region) 
        self._model = TextEmbeddingModel.from_pretrained(self.model_name) #We use the class "TextEmbeddingModel" from the Vertex AI SDK and use its method ".from_pretained" to load a pre'trained embeddings model. 
        

    def process(self, element): #Beam calls the process() method from the DoFn superclass once per element in the PCollection.
        text = f"{element['title']} {element['description']}" #we create a text from the most representative fields of each element in the PCollection. This provide a better semantic meaning than just 1 field.
        try: 
            #We use the self._model object which is an instance of the class  "TextEmbeddingModel" to call the Vertex AI API through the "get_embeddings" method and provide the text. This returns a float vector which represents the semantic meaning of the text.
            embeddings = self._model.get_embeddings([text]) #embeddings is a list of objects. Each object contains metadata and the float vector 
            vector = embeddings[0].values #we take the first object of the embeddings list (and the only one in this case) and obtain the float vector (values)
            yield { #we yield an element with the following structure the the output PCollection.
                "id": element ["id"],
                "url": element["url"],
                "text": text,
                "embedding": vector,
                "processed_at": element["processed_at"],
            }
        except Exception as e: #we catch any exceptions using the superclass "Exception"
            logging.error(f"Embedding failed for title={element['title']}: {e}") #we generate an error log with the info of the element of the PCollection which embedding failed to be generated. Since we are using DirectRunner the log will appear on the terminal. Whe using DataflowRunner the log appears on cloud looging. 


class FormatAsJSONL(beam.DoFn):
    #This function converts an element of the PCollection t a JSON string. 
    def process(self, element):
        yield json.dumps(element) #We yield a JSON string to the output PCollection.
        