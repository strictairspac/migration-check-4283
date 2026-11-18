import {batches,mappings,runs,capacity} from '../api/business.js';
export const store={batches:[],mappings:[],runs:[],capacity:null,user:null};
export async function refresh(){[store.batches,store.mappings,store.runs,store.capacity]=await Promise.all([batches.list(),mappings.list(),runs.list(),capacity.get()]);}
