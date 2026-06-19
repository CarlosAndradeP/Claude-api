import OpenAI from 'openai';



const openai = new OpenAI({

&#x20; apiKey: '$NVIDIA\_API\_KEY',

&#x20; baseURL: 'https://integrate.api.nvidia.com/v1',

})



&#x20;

async function main() {

&#x20; const completion = await openai.chat.completions.create({

&#x20;   model: "nvidia/nemotron-3-super-120b-a12b",

&#x20;   messages: \[{"role":"user","content":""}],

&#x20;   temperature: 1,

&#x20;   top\_p: 0.95,

&#x20;   max\_tokens: 16384,

&#x20;   reasoning\_budget: 16384,

&#x20;   chat\_template\_kwargs: {"enable\_thinking":true},

&#x20;   stream: true

&#x20; })

&#x20;  

&#x20; for await (const chunk of completion) {

&#x20;       const reasoning = chunk.choices\[0]?.delta?.reasoning\_content;

&#x20;   if (reasoning) process.stdout.write(reasoning);

&#x20;       process.stdout.write(chunk.choices\[0]?.delta?.content || '')

&#x20;   

&#x20; }

&#x20; 

}



main();

